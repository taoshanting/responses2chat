package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func convertResponse(body []byte, meta requestMeta) (map[string]any, error) {
	chat, err := decodeJSONObject(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("invalid upstream JSON response: %w", err)
	}
	choices, _ := chat["choices"].([]any)
	if len(choices) == 0 {
		return nil, errors.New("upstream response contains no choices")
	}
	var choice map[string]any
	for _, raw := range choices {
		candidate, ok := object(raw)
		if ok && int64Number(candidate["index"]) == 0 {
			choice = candidate
			break
		}
	}
	if choice == nil {
		return nil, errors.New("upstream response contains no choice at index 0")
	}
	message, ok := object(choice["message"])
	if !ok {
		return nil, errors.New("upstream choice at index 0 contains no message object")
	}
	finishReason, _ := choice["finish_reason"].(string)
	if finishReason == "" {
		return nil, errors.New("upstream response is missing finish_reason")
	}
	status, incomplete, ok := responseStatus(finishReason)
	if !ok {
		return nil, fmt.Errorf("upstream response has unsupported finish_reason %q", finishReason)
	}
	itemStatus := "completed"
	if status == "incomplete" {
		itemStatus = "incomplete"
	}
	output, toolCalls, err := convertChatMessage(message, choice, itemStatus, meta.reasoning)
	if err != nil {
		return nil, err
	}
	if (finishReason == "tool_calls" || finishReason == "function_call") && toolCalls == 0 {
		return nil, fmt.Errorf("upstream response finished with %q but contains no complete function call", finishReason)
	}

	created := int64Number(chat["created"])
	if created == 0 {
		created = time.Now().Unix()
	}
	model, _ := chat["model"].(string)
	if model == "" {
		model, _ = meta.original["model"].(string)
	}
	response := responseEnvelope(meta, responseID(stringValue(chat["id"])), created, model, status, output, convertUsage(chat["usage"]))
	response["incomplete_details"] = incomplete
	if status == "completed" {
		response["completed_at"] = time.Now().Unix()
	}
	if serviceTier, ok := chat["service_tier"]; ok {
		response["service_tier"] = serviceTier
	}
	return response, nil
}

func convertChatMessage(message, choice map[string]any, itemStatus string, reasoningPassthrough bool) ([]any, int, error) {
	output := make([]any, 0)
	if reasoningPassthrough {
		if text := chatReasoningText(message); text != "" {
			output = append(output, reasoningItem(newID("rs"), text, itemStatus))
		}
	}
	contentParts := make([]any, 0, 2)
	if rawContent, exists := message["content"]; exists && rawContent != nil {
		content, ok := rawContent.(string)
		if !ok {
			return nil, 0, errors.New("upstream message content must be a string or null")
		}
		part := map[string]any{
			"type":        "output_text",
			"text":        content,
			"annotations": annotations(message["annotations"]),
		}
		if logprobs := responseLogprobs(choice["logprobs"]); logprobs != nil {
			part["logprobs"] = logprobs
		}
		contentParts = append(contentParts, part)
	}
	if rawRefusal, exists := message["refusal"]; exists && rawRefusal != nil {
		refusal, ok := rawRefusal.(string)
		if !ok {
			return nil, 0, errors.New("upstream message refusal must be a string or null")
		}
		if refusal != "" {
			contentParts = append(contentParts, map[string]any{
				"type":    "refusal",
				"refusal": refusal,
			})
		}
	}
	if len(contentParts) > 0 {
		output = append(output, map[string]any{
			"id":      newID("msg"),
			"type":    "message",
			"status":  itemStatus,
			"role":    "assistant",
			"content": contentParts,
		})
	}

	convertedToolCalls := 0
	rawToolCalls, hasToolCalls := message["tool_calls"]
	toolCalls, ok := rawToolCalls.([]any)
	if hasToolCalls && rawToolCalls != nil && !ok {
		return nil, 0, errors.New("upstream message tool_calls must be an array or null")
	}
	for index, raw := range toolCalls {
		call, ok := object(raw)
		if !ok {
			return nil, 0, fmt.Errorf("upstream message tool_calls[%d] must be an object", index)
		}
		callType := "function"
		if rawType, exists := call["type"]; exists {
			callType, ok = rawType.(string)
			if !ok || callType == "" {
				return nil, 0, fmt.Errorf("upstream message tool_calls[%d].type must be a non-empty string", index)
			}
		}
		if callType != "function" {
			continue
		}
		function, ok := object(call["function"])
		if !ok {
			return nil, 0, fmt.Errorf("upstream message tool_calls[%d].function must be an object", index)
		}
		name, ok := function["name"].(string)
		if !ok || name == "" {
			return nil, 0, fmt.Errorf("upstream message tool_calls[%d].function.name must be a non-empty string", index)
		}
		arguments, ok := function["arguments"].(string)
		if !ok {
			return nil, 0, fmt.Errorf("upstream message tool_calls[%d].function.arguments must be a string", index)
		}
		callID, ok := call["id"].(string)
		if !ok || callID == "" {
			return nil, 0, fmt.Errorf("upstream message tool_calls[%d].id must be a non-empty string", index)
		}
		output = append(output, functionCallOutput(callID, name, arguments, itemStatus))
		convertedToolCalls++
	}
	if rawLegacy, exists := message["function_call"]; exists && rawLegacy != nil {
		legacy, ok := object(rawLegacy)
		if !ok {
			return nil, 0, errors.New("upstream message function_call must be an object or null")
		}
		name, ok := legacy["name"].(string)
		if !ok || name == "" {
			return nil, 0, errors.New("upstream message function_call.name must be a non-empty string")
		}
		arguments, ok := legacy["arguments"].(string)
		if !ok {
			return nil, 0, errors.New("upstream message function_call.arguments must be a string")
		}
		output = append(output, functionCallOutput(newID("call"), name, arguments, itemStatus))
		convertedToolCalls++
	}
	return output, convertedToolCalls, nil
}

func chatReasoningText(message map[string]any) string {
	if text, ok := message["reasoning_content"].(string); ok && text != "" {
		return text
	}
	if text, ok := message["reasoning"].(string); ok && text != "" {
		return text
	}
	return ""
}

func reasoningItem(id, text, status string) map[string]any {
	return map[string]any{
		"id":     id,
		"type":   "reasoning",
		"status": status,
		"summary": []any{
			map[string]any{"type": "summary_text", "text": text},
		},
	}
}

func functionCallOutput(callID, name, arguments, status string) map[string]any {
	return map[string]any{
		"id":        newID("fc"),
		"type":      "function_call",
		"status":    status,
		"call_id":   callID,
		"name":      name,
		"arguments": arguments,
	}
}

func responseEnvelope(meta requestMeta, id string, created int64, model, status string, output []any, usage any) map[string]any {
	original := meta.original
	response := map[string]any{
		"id":                   id,
		"object":               "response",
		"created_at":           created,
		"status":               status,
		"background":           false,
		"error":                nil,
		"incomplete_details":   nil,
		"instructions":         nullable(original, "instructions"),
		"max_output_tokens":    nullable(original, "max_output_tokens"),
		"max_tool_calls":       nil,
		"model":                model,
		"output":               output,
		"parallel_tool_calls":  defaultValue(original, "parallel_tool_calls", true),
		"previous_response_id": nil,
		"reasoning":            responseReasoning(original["reasoning"]),
		"store":                false,
		"temperature":          nullable(original, "temperature"),
		"text":                 responseText(original["text"]),
		"tool_choice":          responseToolChoice(meta),
		"tools":                meta.appliedTools,
		"top_logprobs":         nullable(original, "top_logprobs"),
		"top_p":                nullable(original, "top_p"),
		"truncation":           "disabled",
		"usage":                usage,
		"user":                 nullable(original, "user"),
		"metadata":             defaultValue(original, "metadata", map[string]any{}),
	}
	return response
}

func responseReasoning(value any) map[string]any {
	reasoning, _ := object(value)
	return map[string]any{
		"effort":  nullable(reasoning, "effort"),
		"summary": nil,
	}
}

func responseText(value any) map[string]any {
	text, _ := object(value)
	result := map[string]any{"format": map[string]any{"type": "text"}}
	if format, ok := object(text["format"]); ok {
		formatType, _ := format["type"].(string)
		switch formatType {
		case "text", "json_object", "json_schema":
			result["format"] = format
		}
	}
	if verbosity, ok := text["verbosity"]; ok {
		result["verbosity"] = verbosity
	}
	return result
}

func responseToolChoice(meta requestMeta) any {
	if meta.appliedToolChoice != nil {
		return meta.appliedToolChoice
	}
	return "auto"
}

func convertUsage(value any) any {
	usage, ok := object(value)
	if !ok {
		return nil
	}
	promptDetails, _ := object(usage["prompt_tokens_details"])
	completionDetails, _ := object(usage["completion_tokens_details"])
	return map[string]any{
		"input_tokens": int64Number(usage["prompt_tokens"]),
		"input_tokens_details": map[string]any{
			"cached_tokens":      int64Number(promptDetails["cached_tokens"]),
			"cache_write_tokens": int64Number(promptDetails["cache_write_tokens"]),
		},
		"output_tokens": int64Number(usage["completion_tokens"]),
		"output_tokens_details": map[string]any{
			"reasoning_tokens": int64Number(completionDetails["reasoning_tokens"]),
		},
		"total_tokens": int64Number(usage["total_tokens"]),
	}
}

func responseLogprobs(value any) any {
	logprobs, ok := object(value)
	if !ok {
		return nil
	}
	content, ok := logprobs["content"].([]any)
	if !ok {
		return nil
	}
	return content
}

func annotations(value any) []any {
	items, ok := value.([]any)
	if !ok {
		return []any{}
	}
	converted := make([]any, 0, len(items))
	for _, raw := range items {
		annotation, ok := object(raw)
		if !ok || annotation["type"] != "url_citation" {
			continue
		}
		source := annotation
		if nested, ok := object(annotation["url_citation"]); ok {
			source = nested
		}
		urlValue, _ := source["url"].(string)
		if urlValue == "" {
			continue
		}
		citation := map[string]any{
			"type":        "url_citation",
			"url":         urlValue,
			"title":       stringValue(source["title"]),
			"start_index": int64Number(source["start_index"]),
			"end_index":   int64Number(source["end_index"]),
		}
		converted = append(converted, citation)
	}
	return converted
}

func responseStatus(reason string) (string, any, bool) {
	switch reason {
	case "length":
		return "incomplete", map[string]any{"reason": "max_output_tokens"}, true
	case "content_filter":
		return "incomplete", map[string]any{"reason": "content_filter"}, true
	case "stop", "tool_calls", "function_call":
		return "completed", nil, true
	default:
		return "", nil, false
	}
}

func nullable(values map[string]any, key string) any {
	if values == nil {
		return nil
	}
	return values[key]
}

func defaultValue(values map[string]any, key string, fallback any) any {
	if values != nil {
		if value, ok := values[key]; ok && value != nil {
			return value
		}
	}
	return fallback
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func int64Number(value any) int64 {
	switch number := value.(type) {
	case json.Number:
		result, _ := number.Int64()
		return result
	case float64:
		return int64(number)
	case int64:
		return number
	case int:
		return int64(number)
	default:
		return 0
	}
}
