
RULES_DIR ?= ./deployment/docker/rules

format-rules:
	find $(RULES_DIR) -type f -name "*.yml" -print0 | xargs -0 -I {} \
		echo "Processing {}" && yq -i '.groups[].rules[].annotations |= (.summary style="literal", .description style="literal")' "{}"

format-rules-dry:
	find $(RULES_DIR) -type f -name "*.yml" -print0 | xargs -0 -I {} sh -c 'echo -e "\n===== {} ====="; yq ".groups[].rules[].annotations.summary style=\"literal\" | .groups[].rules[].annotations.description style=\"literal\"" "{}"'