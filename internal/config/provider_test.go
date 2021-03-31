package config

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestConfigByEnv(t *testing.T) {

}

func TestNew(t *testing.T) {
	//t.Run("测试当传入的config为空值或复制子元素为空时", func(t *testing.T) {
	//	var c Config
	//	provide := New(c, true, "TL")
	//	assert.EqualValues(t, 17, len(provide.(*Provide).tags))
	//})
	t.Run("测试正常传值的时候", func(t *testing.T) {
		_ = &Config{
			Cluster: ClusterConfig{
				NodeName:            "wwd",
				ListenAddr:          "wdada",
				GossipInterval:      23,
				GossipCheckInterval: 22,
				Seed:                []string{"1", "2", "3", "4"},
			},
			Broker: BrokerConfig{ListenAddr: "wdwdds"},
			Chan: ChanConfig{
				LocalBatchTopicOpChanCoreSize:  12,
				QueryRequestChanCoreSize:       12,
				LocalPubMessageChanCoreSize:    12,
				RemotePubMessageChanCoreSize:   12,
				RemoteBatchTopicOpChanCoreSize: 12,
			},
			LogLevel: "wdwad",
		}
		//provide := New(c, true, "TL")
		//assert.EqualValues(t, 17, len(provide.(*Provide).tags))
	})
}
func TestProvide_ProvideByEnv(t *testing.T) {
	t.Run("测试大小写不区分的情况", func(t *testing.T) {
		c := &Config{
			Cluster: ClusterConfig{
				NodeName:            "wwd",
				ListenAddr:          "wdada",
				GossipInterval:      23,
				GossipCheckInterval: 22,
				Seed:                []string{"1", "2", "3", "4"},
			},
			Broker: BrokerConfig{ListenAddr: "wdwdds"},
			Chan: ChanConfig{
				LocalBatchTopicOpChanCoreSize:  12,
				QueryRequestChanCoreSize:       12,
				LocalPubMessageChanCoreSize:    12,
				RemotePubMessageChanCoreSize:   12,
				RemoteBatchTopicOpChanCoreSize: 12,
			},
			LogLevel: "wdwad",
		}
		_ = os.Setenv("TL_CLuster_GossipInterval", "233ns")
		_ = os.Setenv("TL_CLuster_Seed", ":4001,:4002,:4003")
		err := FromEnv(c, true, "TL")
		assert.NoError(t, err)
		assert.EqualValues(t, 233*time.Nanosecond, c.Cluster.GossipInterval)
		assert.EqualValues(t, 3, len(c.Cluster.Seed))
	})
	t.Run("测试大小写区分的情况", func(t *testing.T) {
		c := &Config{
			Cluster: ClusterConfig{
				NodeName:            "wwd",
				ListenAddr:          "wdada",
				GossipInterval:      23,
				GossipCheckInterval: 22,
				Seed:                []string{"1", "2", "3", "4"},
			},
			Broker: BrokerConfig{ListenAddr: "wdwdds"},
			Chan: ChanConfig{
				LocalBatchTopicOpChanCoreSize:  12,
				QueryRequestChanCoreSize:       12,
				LocalPubMessageChanCoreSize:    12,
				RemotePubMessageChanCoreSize:   12,
				RemoteBatchTopicOpChanCoreSize: 12,
			},
			LogLevel: "wdwad",
		}
		_ = os.Setenv("TL_CLUster_GossipInterval", "233")
		_ = os.Setenv("TL_cluster_gossipCheckInterval", "23s")
		err := FromEnv(c, false, "TL")
		assert.NoError(t, err)
		assert.EqualValues(t, 23, c.Cluster.GossipInterval)
		assert.EqualValues(t, 23*time.Second, c.Cluster.GossipCheckInterval)
	})
}

func TestFromFlag(t *testing.T) {
	t.Run("测试大小写不区分的情况", func(t *testing.T) {
		c := &Config{
			Cluster: ClusterConfig{
				NodeName:            "wwd",
				ListenAddr:          "wdada",
				GossipInterval:      23,
				GossipCheckInterval: 22,
				Seed:                []string{"1", "2", "3", "4"},
			},
			Broker: BrokerConfig{ListenAddr: "wdwdds"},
			Chan: ChanConfig{
				LocalBatchTopicOpChanCoreSize:  12,
				QueryRequestChanCoreSize:       12,
				LocalPubMessageChanCoreSize:    12,
				RemotePubMessageChanCoreSize:   12,
				RemoteBatchTopicOpChanCoreSize: 12,
			},
			LogLevel: "wdwad",
		}
		keyAndValues := make([]string, 0)
		keyAndValues = append(keyAndValues, "TL_chan_subscribeChanSize=21")
		keyAndValues = append(keyAndValues, "TL_cluster_nodeName=00:00:00:00:00:02")
		keyAndValues = append(keyAndValues, "TL_broker_listenAddr=:4002")
		keyAndValues = append(keyAndValues, "TL_cluster_seed=:4002,:4001")
		err := FromFlag(c, true, "TL", keyAndValues)
		assert.NoError(t, err)
		assert.EqualValues(t, "00:00:00:00:00:02", c.Cluster.NodeName)
		assert.EqualValues(t, ":4001", c.Cluster.Seed[1])
	})
}

func TestProvide_ProvideByYamlFile(t *testing.T) {
	t.Run("测试yaml文件配置的情况", func(t *testing.T) {
		c := &Config{
			Cluster: ClusterConfig{
				NodeName:            "wwd",
				ListenAddr:          "wdada",
				GossipInterval:      23,
				GossipCheckInterval: 22,
				Seed:                []string{"1", "2", "3", "4"},
			},
			Broker: BrokerConfig{ListenAddr: "wdwdds"},
			Chan: ChanConfig{
				LocalBatchTopicOpChanCoreSize:  12,
				QueryRequestChanCoreSize:       12,
				LocalPubMessageChanCoreSize:    12,
				RemotePubMessageChanCoreSize:   12,
				RemoteBatchTopicOpChanCoreSize: 12,
			},
			LogLevel: "wdwad",
		}
		err := FromYaml(c, "provide_test.yaml")
		assert.NoError(t, err)
		assert.EqualValues(t, 23*time.Second, c.Cluster.GossipInterval)
	})
	t.Run("测试yaml文件配置中出现bool配置的情况", func(t *testing.T) {
		type Test2 struct {
			C *Config `yaml:"c"`
			B bool    `yaml:"b"`
		}
		c := &Config{
			Cluster: ClusterConfig{
				NodeName:            "wwd",
				ListenAddr:          "wdada",
				GossipInterval:      23,
				GossipCheckInterval: 22,
				Seed:                []string{"1", "2", "3", "4"},
			},
			Broker: BrokerConfig{ListenAddr: "wdwdds"},
			Chan: ChanConfig{
				LocalBatchTopicOpChanCoreSize:  12,
				QueryRequestChanCoreSize:       12,
				LocalPubMessageChanCoreSize:    12,
				RemotePubMessageChanCoreSize:   12,
				RemoteBatchTopicOpChanCoreSize: 12,
			},
			LogLevel: "wdwad",
		}
		Test := &Test2{
			C: c,
			B: true,
		}
		_ = FromYaml(Test, "provide2_test.yaml")
		assert.EqualValues(t, 23*time.Second, c.Cluster.GossipInterval)
	})
}

func TestProvide_Unmarshal(t *testing.T) {
	t.Run("复杂类型,拥有各种复杂类型", func(t *testing.T) {
		c := &Config{
			Cluster: ClusterConfig{
				NodeName:            "wwd",
				ListenAddr:          "wdada",
				GossipInterval:      23 * time.Second,
				GossipCheckInterval: 22,
				Seed:                []string{"1", "2", "3", "4"},
			},
			Broker: BrokerConfig{ListenAddr: "wdwdds"},
			Chan: ChanConfig{
				LocalBatchTopicOpChanCoreSize:  12,
				QueryRequestChanCoreSize:       12,
				LocalPubMessageChanCoreSize:    12,
				RemotePubMessageChanCoreSize:   12,
				RemoteBatchTopicOpChanCoreSize: 12,
			},
			LogLevel: "wdwad",
		}
		_ = FromYaml(c, "provide_test.yaml")
		assert.EqualValues(t, time.Second*23, c.Cluster.GossipInterval)
	})
}
