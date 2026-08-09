package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

type requestMeta struct {
	original          map[string]any
	appliedTools      []any
	appliedToolChoice any
	stream            bool
	reasoning         bool
}

const maxModelNameBytes = 512

func convertRequest(body []byte, reasoningPassthrough bool) (map[string]any, requestMeta, error) {
	src, err := decodeJSONObject(bytes.NewReader(body))
	if err != nil {
		return nil, requestMeta{}, fmt.Errorf("invalid JSON request: %w", err)
	}
	model, ok := src["model"].(string)
	if !ok || strings.TrimSpace(model) == "" {
		return nil, requestMeta{}, errors.New("model must be a non-empty string")
	}
	if len(model) > maxModelNameBytes {
		return nil, requestMeta{}, fmt.Errorf("model must not exceed %d bytes", maxModelNameBytes)
	}

	dst := make(map[string]any)
	meta := requestMeta{original: src, reasoning: reasoningPassthrough}
	copyFields(src, dst,
		"model", "parallel_tool_calls", "temperature", "top_p",
		"prompt_cache_key", "safety_identifier", "user", "service_tier", "metadata",
	)
	// Chat's store flag cannot implement Responses object retrieval or
	// previous_response_id chaining, especially because response IDs are rewritten.
	// Disable storage explicitly instead of advertising state the gateway cannot use.
	dst["store"] = false

	messages := make([]any, 0)
	if instructions, ok := src["instructions"].(string); ok {
		messages = append(messages, map[string]any{
			"role":    "developer",
			"content": instructions,
		})
	}
	if input, ok := src["input"]; ok && input != nil {
		converted, err := convertInput(input, reasoningPassthrough)
		if err != nil {
			return nil, requestMeta{}, err
		}
		messages = append(messages, converted...)
	}
	dst["messages"] = messages

	if value, ok := src["max_output_tokens"]; ok {
		dst["max_completion_tokens"] = value
	}
	if topLogprobs, ok := src["top_logprobs"]; ok {
		dst["top_logprobs"] = topLogprobs
		dst["logprobs"] = true
	}
	if reasoning, ok := object(src["reasoning"]); ok {
		if effort, ok := reasoning["effort"]; ok && effort != nil {
			dst["reasoning_effort"] = effort
		}
	}
	if text, ok := object(src["text"]); ok {
		if verbosity, ok := text["verbosity"]; ok && verbosity != nil {
			dst["verbosity"] = verbosity
		}
		if format, ok := object(text["format"]); ok {
			if converted, ok := convertTextFormat(format); ok {
				dst["response_format"] = converted
			}
		}
	}

	tools, err := convertTools(src["tools"])
	if err != nil {
		return nil, requestMeta{}, err
	}
	if len(tools.chat) > 0 {
		dst["tools"] = tools.chat
	}
	meta.appliedTools = tools.responses
	choice, choiceOK, err := convertToolChoice(src["tool_choice"], tools)
	if err != nil {
		return nil, requestMeta{}, err
	}
	if choiceOK {
		dst["tool_choice"] = choice.chat
		meta.appliedToolChoice = choice.responses
	}

	if stream, _ := src["stream"].(bool); stream {
		meta.stream = true
		dst["stream"] = true
		// Responses requires usage in its terminal event. Chat streams only send it
		// when explicitly requested.
		dst["stream_options"] = map[string]any{"include_usage": true}
	}

	return dst, meta, nil
}

func copyFields(src, dst map[string]any, fields ...string) {
	for _, field := range fields {
		if value, ok := src[field]; ok && value != nil {
			dst[field] = value
		}
	}
}

func object(value any) (map[string]any, bool) {
	v, ok := value.(map[string]any)
	return v, ok
}

func convertInput(input any, reasoningPassthrough bool) ([]any, error) {
	switch value := input.(type) {
	case string:
		return []any{map[string]any{"role": "user", "content": value}}, nil
	case []any:
		return convertItems(value, reasoningPassthrough)
	default:
		return nil, errors.New("input must be a string or an array of input items")
	}
}

func convertItems(items []any, reasoningPassthrough bool) ([]any, error) {
	messages := make([]any, 0, len(items))
	lastAssistant := -1
	pendingReasoning := ""

	attachReasoning := func(assistant map[string]any) {
		if pendingReasoning == "" {
			return
		}
		if assistant["reasoning_content"] == nil {
			assistant["reasoning_content"] = pendingReasoning
		}
		pendingReasoning = ""
	}

	for index, raw := range items {
		item, ok := object(raw)
		if !ok {
			return nil, fmt.Errorf("input[%d] must be an object", index)
		}
		path := fmt.Sprintf("input[%d]", index)
		itemType := ""
		if rawType, exists := item["type"]; exists {
			var typeOK bool
			itemType, typeOK = rawType.(string)
			if !typeOK || itemType == "" {
				return nil, fmt.Errorf("%s.type must be a non-empty string", path)
			}
		} else if _, hasRole := item["role"]; hasRole {
			itemType = "message"
		} else {
			return nil, fmt.Errorf("%s must contain type or role", path)
		}

		switch itemType {
		case "message":
			message, ok, err := convertMessage(item, path)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			messages = append(messages, message)
			if message["role"] == "assistant" {
				lastAssistant = len(messages) - 1
				attachReasoning(message)
			} else {
				lastAssistant = -1
				pendingReasoning = ""
			}
		case "function_call":
			call, err := convertFunctionCall(item, path)
			if err != nil {
				return nil, err
			}
			if lastAssistant < 0 {
				assistant := map[string]any{
					"role":       "assistant",
					"tool_calls": []any{call},
				}
				attachReasoning(assistant)
				messages = append(messages, assistant)
				lastAssistant = len(messages) - 1
			} else {
				assistant := messages[lastAssistant].(map[string]any)
				calls, _ := assistant["tool_calls"].([]any)
				assistant["tool_calls"] = append(calls, call)
				attachReasoning(assistant)
			}
		case "function_call_output":
			message, err := convertFunctionOutput(item, path)
			if err != nil {
				return nil, err
			}
			messages = append(messages, message)
			lastAssistant = -1
			pendingReasoning = ""
		case "reasoning":
			if !reasoningPassthrough {
				continue
			}
			// A reasoning Item precedes the assistant message or function call it
			// belongs to; hold its text and attach it to that assistant message as
			// the non-standard reasoning_content field understood by reasoning-model
			// Chat upstreams.
			if text := reasoningItemText(item); text != "" {
				if pendingReasoning != "" {
					pendingReasoning += "\n"
				}
				pendingReasoning += text
			}
		default:
			// Hosted/MCP/computer/shell/image-generation calls, item references,
			// approvals and any future Items have no faithful Chat Completions
			// message representation. They are intentionally dropped.
		}
	}
	return messages, nil
}

func reasoningItemText(item map[string]any) string {
	collect := func(value any, partType, field string) string {
		parts, _ := value.([]any)
		texts := make([]string, 0, len(parts))
		for _, raw := range parts {
			part, ok := object(raw)
			if !ok || part["type"] != partType {
				continue
			}
			if text, ok := part[field].(string); ok && text != "" {
				texts = append(texts, text)
			}
		}
		return strings.Join(texts, "\n")
	}
	if text := collect(item["content"], "reasoning_text", "text"); text != "" {
		return text
	}
	return collect(item["summary"], "summary_text", "text")
}

func convertMessage(item map[string]any, path string) (map[string]any, bool, error) {
	role, roleOK := item["role"].(string)
	if !roleOK || role == "" {
		return nil, false, fmt.Errorf("%s.role must be a supported non-empty string", path)
	}
	switch role {
	case "user", "assistant", "system", "developer":
	default:
		return nil, false, fmt.Errorf("%s.role %q is not supported", path, role)
	}

	content, ok, err := convertContent(item["content"], role, path+".content")
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	message := map[string]any{"role": role, "content": content}
	if rawName, exists := item["name"]; exists && rawName != nil {
		name, ok := rawName.(string)
		if !ok {
			return nil, false, fmt.Errorf("%s.name must be a string", path)
		}
		if name != "" {
			message["name"] = name
		}
	}
	return message, true, nil
}

func convertContent(value any, role, path string) (any, bool, error) {
	if text, ok := value.(string); ok {
		return text, true, nil
	}
	parts, ok := value.([]any)
	if !ok {
		return nil, false, fmt.Errorf("%s must be a string or an array", path)
	}

	converted := make([]any, 0, len(parts))
	for index, raw := range parts {
		part, ok := object(raw)
		if !ok {
			return nil, false, fmt.Errorf("%s[%d] must be an object", path, index)
		}
		partPath := fmt.Sprintf("%s[%d]", path, index)
		partType, ok := part["type"].(string)
		if !ok || partType == "" {
			return nil, false, fmt.Errorf("%s.type must be a non-empty string", partPath)
		}
		switch partType {
		case "input_text", "output_text", "text":
			text, ok := part["text"].(string)
			if !ok {
				return nil, false, fmt.Errorf("%s.text must be a string", partPath)
			}
			converted = append(converted, map[string]any{"type": "text", "text": text})
		case "refusal":
			if role == "assistant" {
				refusal, ok := part["refusal"].(string)
				if !ok {
					return nil, false, fmt.Errorf("%s.refusal must be a string", partPath)
				}
				converted = append(converted, map[string]any{"type": "refusal", "refusal": refusal})
			}
		case "input_image", "image_url":
			if role != "user" {
				continue
			}
			image, err := convertImagePart(part, partPath)
			if err != nil {
				return nil, false, err
			}
			if image != nil {
				converted = append(converted, image)
			}
		case "input_file", "file":
			if role != "user" {
				continue
			}
			file, err := convertFilePart(part, partPath)
			if err != nil {
				return nil, false, err
			}
			if file != nil {
				converted = append(converted, file)
			}
		}
	}
	if len(converted) == 0 {
		return nil, false, nil
	}
	return converted, true, nil
}

func convertImagePart(part map[string]any, path string) (map[string]any, error) {
	if rawURL, exists := part["image_url"]; exists && rawURL != nil {
		if nested, ok := object(rawURL); ok {
			url, ok := nested["url"].(string)
			if !ok || url == "" {
				return nil, fmt.Errorf("%s.image_url.url must be a non-empty string", path)
			}
			imageURL := map[string]any{"url": url}
			if rawDetail, exists := nested["detail"]; exists && rawDetail != nil {
				detail, ok := rawDetail.(string)
				if !ok {
					return nil, fmt.Errorf("%s.image_url.detail must be a string", path)
				}
				if detail != "" {
					imageURL["detail"] = detail
				}
			}
			return map[string]any{"type": "image_url", "image_url": imageURL}, nil
		}
		url, ok := rawURL.(string)
		if !ok || url == "" {
			return nil, fmt.Errorf("%s.image_url must be a non-empty string", path)
		}
		imageURL := map[string]any{"url": url}
		if rawDetail, exists := part["detail"]; exists && rawDetail != nil {
			detail, ok := rawDetail.(string)
			if !ok {
				return nil, fmt.Errorf("%s.detail must be a string", path)
			}
			if detail != "" {
				imageURL["detail"] = detail
			}
		}
		return map[string]any{"type": "image_url", "image_url": imageURL}, nil
	}
	if rawFileID, exists := part["file_id"]; exists {
		fileID, ok := rawFileID.(string)
		if !ok || fileID == "" {
			return nil, fmt.Errorf("%s.file_id must be a non-empty string", path)
		}
		// Chat Completions cannot address a Responses image by file_id.
		return nil, nil
	}
	return nil, fmt.Errorf("%s must contain image_url or file_id", path)
}

func convertFilePart(part map[string]any, path string) (map[string]any, error) {
	if rawFile, exists := part["file"]; exists && rawFile != nil {
		nested, ok := object(rawFile)
		if !ok {
			return nil, fmt.Errorf("%s.file must be an object", path)
		}
		part = nested
		path += ".file"
	}
	file := make(map[string]any)
	for _, field := range []string{"file_id", "file_data", "filename"} {
		raw, exists := part[field]
		if !exists {
			continue
		}
		value, ok := raw.(string)
		if !ok || value == "" {
			return nil, fmt.Errorf("%s.%s must be a non-empty string", path, field)
		}
		file[field] = value
	}
	if file["file_id"] == nil && file["file_data"] == nil {
		if rawURL, exists := part["file_url"]; exists {
			fileURL, ok := rawURL.(string)
			if !ok || fileURL == "" {
				return nil, fmt.Errorf("%s.file_url must be a non-empty string", path)
			}
			// file_url is valid in Responses, but Chat Completions has no equivalent.
			return nil, nil
		}
		return nil, fmt.Errorf("%s must contain file_id, file_data, or file_url", path)
	}
	return map[string]any{"type": "file", "file": file}, nil
}

func convertFunctionCall(item map[string]any, path string) (map[string]any, error) {
	name, ok := item["name"].(string)
	if !ok || name == "" {
		return nil, fmt.Errorf("%s.name must be a non-empty string", path)
	}
	arguments, ok := item["arguments"].(string)
	if !ok {
		return nil, fmt.Errorf("%s.arguments must be a string", path)
	}
	callID := ""
	if rawCallID, exists := item["call_id"]; exists {
		var callIDOK bool
		callID, callIDOK = rawCallID.(string)
		if !callIDOK || callID == "" {
			return nil, fmt.Errorf("%s.call_id must be a non-empty string", path)
		}
	} else if rawID, exists := item["id"]; exists {
		var idOK bool
		callID, idOK = rawID.(string)
		if !idOK || callID == "" {
			return nil, fmt.Errorf("%s.id must be a non-empty string", path)
		}
	}
	if callID == "" {
		return nil, fmt.Errorf("%s.call_id must be a non-empty string", path)
	}
	return map[string]any{
		"id":   callID,
		"type": "function",
		"function": map[string]any{
			"name":      name,
			"arguments": arguments,
		},
	}, nil
}

func convertFunctionOutput(item map[string]any, path string) (map[string]any, error) {
	callID, _ := item["call_id"].(string)
	if callID == "" {
		return nil, fmt.Errorf("%s.call_id must be a non-empty string", path)
	}
	output, exists := item["output"]
	if !exists {
		return nil, fmt.Errorf("%s.output is required", path)
	}
	content, err := convertToolOutput(output, path+".output")
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"role":         "tool",
		"tool_call_id": callID,
		"content":      content,
	}, nil
}

func convertToolOutput(value any, path string) (any, error) {
	if text, ok := value.(string); ok {
		return text, nil
	}
	parts, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a string or an array", path)
	}
	converted := make([]any, 0, len(parts))
	for index, raw := range parts {
		part, ok := object(raw)
		if !ok {
			return nil, fmt.Errorf("%s[%d] must be an object", path, index)
		}
		partType, ok := part["type"].(string)
		if !ok || partType == "" {
			return nil, fmt.Errorf("%s[%d].type must be a non-empty string", path, index)
		}
		if partType == "input_text" || partType == "text" {
			text, ok := part["text"].(string)
			if !ok {
				return nil, fmt.Errorf("%s[%d].text must be a string", path, index)
			}
			converted = append(converted, map[string]any{"type": "text", "text": text})
		}
	}
	if len(converted) == 0 {
		return "", nil
	}
	return converted, nil
}

type convertedTools struct {
	chat      []any
	responses []any
}

func convertTools(value any) (convertedTools, error) {
	if value == nil {
		return convertedTools{chat: []any{}, responses: []any{}}, nil
	}
	items, ok := value.([]any)
	if !ok {
		return convertedTools{}, errors.New("tools must be an array")
	}
	result := convertedTools{
		chat:      make([]any, 0, len(items)),
		responses: make([]any, 0, len(items)),
	}
	for index, raw := range items {
		tool, ok := object(raw)
		if !ok {
			return convertedTools{}, fmt.Errorf("tools[%d] must be an object", index)
		}
		toolType, ok := tool["type"].(string)
		if !ok || toolType == "" {
			return convertedTools{}, fmt.Errorf("tools[%d].type must be a non-empty string", index)
		}
		if toolType != "function" {
			continue
		}
		name, ok := tool["name"].(string)
		if !ok || name == "" {
			return convertedTools{}, fmt.Errorf("tools[%d].name must be a non-empty string", index)
		}
		if description, exists := tool["description"]; exists && description != nil {
			if _, ok := description.(string); !ok {
				return convertedTools{}, fmt.Errorf("tools[%d].description must be a string", index)
			}
		}
		if parameters, exists := tool["parameters"]; exists && parameters != nil {
			if _, ok := object(parameters); !ok {
				return convertedTools{}, fmt.Errorf("tools[%d].parameters must be an object", index)
			}
		}
		if strict, exists := tool["strict"]; exists && strict != nil {
			if _, ok := strict.(bool); !ok {
				return convertedTools{}, fmt.Errorf("tools[%d].strict must be a boolean", index)
			}
		}
		definition := map[string]any{"name": name}
		copyFields(tool, definition, "description", "parameters", "strict")
		result.chat = append(result.chat, map[string]any{
			"type":     "function",
			"function": definition,
		})
		responseTool := map[string]any{"type": "function", "name": name}
		copyFields(tool, responseTool, "description", "parameters", "strict")
		result.responses = append(result.responses, responseTool)
	}
	return result, nil
}

type convertedChoice struct {
	chat      any
	responses any
}

func convertToolChoice(value any, tools convertedTools) (convertedChoice, bool, error) {
	if value == nil {
		return convertedChoice{}, false, nil
	}
	hasTools := len(tools.chat) > 0
	if mode, ok := value.(string); ok {
		switch mode {
		case "none":
			return convertedChoice{chat: mode, responses: mode}, true, nil
		case "auto", "required":
			if !hasTools {
				return convertedChoice{chat: "none", responses: "none"}, true, nil
			}
			return convertedChoice{chat: mode, responses: mode}, true, nil
		}
		return convertedChoice{}, false, fmt.Errorf("unsupported tool_choice %q", mode)
	}
	choice, ok := object(value)
	if !ok {
		return convertedChoice{}, false, errors.New("tool_choice must be a string or an object")
	}
	choiceType, ok := choice["type"].(string)
	if !ok || choiceType == "" {
		return convertedChoice{}, false, errors.New("tool_choice.type must be a non-empty string")
	}
	if choiceType != "function" {
		return convertedChoice{}, false, nil
	}
	name, ok := choice["name"].(string)
	if !ok || name == "" {
		return convertedChoice{}, false, errors.New("tool_choice.name must be a non-empty string")
	}
	if !hasTools {
		return convertedChoice{}, false, errors.New("tool_choice requires a supported function tool")
	}
	found := false
	for _, raw := range tools.responses {
		tool, _ := object(raw)
		if tool["name"] == name {
			found = true
			break
		}
	}
	if !found {
		return convertedChoice{}, false, fmt.Errorf("tool_choice names undefined function %q", name)
	}
	return convertedChoice{
		chat: map[string]any{
			"type":     "function",
			"function": map[string]any{"name": name},
		},
		responses: map[string]any{"type": "function", "name": name},
	}, true, nil
}

func convertTextFormat(format map[string]any) (map[string]any, bool) {
	formatType, _ := format["type"].(string)
	switch formatType {
	case "", "text":
		return nil, false
	case "json_object":
		return map[string]any{"type": "json_object"}, true
	case "json_schema":
		schema := make(map[string]any)
		copyFields(format, schema, "name", "description", "schema", "strict")
		if schema["name"] == nil || schema["schema"] == nil {
			return nil, false
		}
		return map[string]any{"type": "json_schema", "json_schema": schema}, true
	default:
		return nil, false
	}
}
