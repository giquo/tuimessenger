package bridges

import (
	mattermost "github.com/bostrot/mautrix-mattermost/pkg/connector"

	"github.com/whyyor/tuimessenger/internal/config"
)

func init() {
	register("mattermost", newMattermost)
}

func newMattermost(_ *config.Config) (*Network, error) {
	connector := &mattermost.MattermostConnector{}
	_, err := loadDefaults[mattermost.Config](connector)
	if err != nil {
		return nil, err
	}
	return &Network{Connector: connector, Config: baseConfig("!mm")}, nil
}
