package db

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
)

func buildIAMAuthToken(ctx context.Context, host, port, user, region string) (string, error) {
	endpoint := fmt.Sprintf("%s:%s", host, port)
	var cfgOpts []func(*config.LoadOptions) error
	if region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return "", fmt.Errorf("failed to load AWS config for IAM auth: %w", err)
	}
	if cfg.Region == "" {
		return "", fmt.Errorf("AWS region not set: cannot determine region from host %q and AWS config has no region (set AWS_REGION or use standard RDS endpoint like db.x.region.rds.amazonaws.com)", host)
	}
	token, err := auth.BuildAuthToken(ctx, endpoint, cfg.Region, user, cfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to build IAM auth token: %w", err)
	}
	return token, nil
}
