package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

var configFile = flag.String("config.file", "", "Path to vlagent YAML configuration file")

func loadConfigFileFromArgs() error {
	configPath := *configFile
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if strings.HasPrefix(arg, "-config.file=") || strings.HasPrefix(arg, "--config.file=") {
			configPath = strings.SplitN(arg, "=", 2)[1]
			break
		}
		if (arg == "-config.file" || arg == "--config.file") && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
			break
		}
	}
	if configPath == "" {
		return nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("cannot read config file %q: %w", configPath, err)
	}

	var raw interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("cannot parse config file %q: %w", configPath, err)
	}
	root, ok := normalizeConfigValue(raw).(map[string]interface{})
	if !ok {
		return fmt.Errorf("config file %q must contain a YAML object", configPath)
	}
	if nested, ok := root["config"].(map[string]interface{}); ok {
		root = nested
	}

	if err := setConfigFlag(root, "tmpDataPath", "tmpDataPath"); err != nil {
		return err
	}

	if collector, ok := configMap(root["collector"]); ok {
		if err := setConfigFlag(collector, "enabled", "kubernetesCollector"); err != nil {
			return err
		}
		for _, name := range []string{
			"timeField",
			"msgField",
			"streamFields",
			"excludeFilter",
			"includePodLabels",
			"includePodAnnotations",
			"includeNodeLabels",
			"includeNodeAnnotations",
		} {
			if err := setConfigFlag(collector, name, "kubernetesCollector."+name); err != nil {
				return err
			}
		}
	}

	if remoteWrites, ok := configList(root["remoteWrite"]); ok {
		for _, item := range remoteWrites {
			remoteWrite, ok := configMap(item)
			if !ok {
				return fmt.Errorf("remoteWrite entries must be YAML objects")
			}
			for name, value := range remoteWrite {
				if name == "persistence" {
					continue
				}
				if err := setConfigFlag(map[string]interface{}{"value": value}, "value", "remoteWrite."+name); err != nil {
					return err
				}
			}
		}
	}

	if localStorage, ok := configMap(root["localStorage"]); ok {
		for name, value := range localStorage {
			if name == "persistence" || name == "virtLauncher" {
				continue
			}
			if err := setConfigFlag(map[string]interface{}{"value": value}, "value", "localStorage."+name); err != nil {
				return err
			}
		}
		if virtLauncher, ok := configMap(localStorage["virtLauncher"]); ok {
			for name, value := range virtLauncher {
				if err := setConfigFlag(map[string]interface{}{"value": value}, "value", "localStorage.virtLauncher."+name); err != nil {
					return err
				}
			}
		}
	}

	if csi, ok := configMap(root["logConfigCSI"]); ok {
		csiFlags := map[string]string{
			"rbdHdd":  "kubernetesCollector.csiRbdHdd",
			"rbdSsd":  "kubernetesCollector.csiRbdSsd",
			"rbdNvme": "kubernetesCollector.csiRbdNvme",
		}
		for name, flagName := range csiFlags {
			if err := setConfigFlag(csi, name, flagName); err != nil {
				return err
			}
		}
	}

	return nil
}

func setConfigFlag(values map[string]interface{}, valueName, flagName string) error {
	value, ok := values[valueName]
	if !ok {
		return nil
	}
	if list, ok := value.([]interface{}); ok {
		if len(list) == 0 {
			return nil
		}
		items := make([]string, 0, len(list))
		for _, item := range list {
			items = append(items, fmt.Sprint(item))
		}
		value = strings.Join(items, ",")
	}
	if err := flag.Set(flagName, fmt.Sprint(value)); err != nil {
		return fmt.Errorf("cannot apply config value %q: %w", flagName, err)
	}
	return nil
}

func configMap(value interface{}) (map[string]interface{}, bool) {
	result, ok := value.(map[string]interface{})
	return result, ok
}

func configList(value interface{}) ([]interface{}, bool) {
	result, ok := value.([]interface{})
	return result, ok
}

func normalizeConfigValue(value interface{}) interface{} {
	switch value := value.(type) {
	case map[interface{}]interface{}:
		result := make(map[string]interface{}, len(value))
		for key, item := range value {
			result[fmt.Sprint(key)] = normalizeConfigValue(item)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(value))
		for i, item := range value {
			result[i] = normalizeConfigValue(item)
		}
		return result
	default:
		return value
	}
}
