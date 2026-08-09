package updater

import (
	"context"
	"fmt"
	"os"

	"github.com/lieyan/responses2chat/internal/version"
)

// RunOnce performs a synchronous check-download-apply cycle for CLI usage
// (`responses2chat update`). Invoking it counts as explicit confirmation, so
// dev-channel builds are applied immediately instead of parking in "ready".
// On Unix a successful apply never returns: the process image is replaced.
func (u *Updater) RunOnce(ctx context.Context) error {
	cfg := normalizeConfig(u.cfg())

	u.logger.Printf("update: current version %s (%s), channel=%s, source=%s", version.Version, version.Commit, cfg.Channel, cfg.Source)

	release, hasUpdate, err := u.checkForUpdate(ctx, cfg)
	if err != nil {
		return fmt.Errorf("check failed: %w", err)
	}
	// A nil release ("no release found") is already logged by the fetcher.
	if release == nil || !hasUpdate {
		return nil
	}

	u.logger.Printf("update: %s available, downloading", release.displayVersion())
	binaryPath, err := u.download(ctx, cfg, release)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	// Successful exec/exit never returns; any returned failure should not leave
	// a verified-but-unapplied binary accumulating in the cache.
	defer func() { _ = os.Remove(binaryPath) }()

	if err := u.waitForIdle(ctx); err != nil {
		return fmt.Errorf("apply canceled while waiting for idle: %w", err)
	}
	if err := u.applyUpdate(binaryPath, release.TagName); err != nil {
		u.notifyExecFailure(err)
		return fmt.Errorf("apply failed: %w", err)
	}
	return nil
}
