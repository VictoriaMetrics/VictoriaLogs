
RULES_DIR ?= ./deployment/docker/rules

format-rules:
	find $(RULES_DIR) -type f -name "*.yml" | while IFS= read -r f; do \
		echo "Processing $${f}"; \
		yq -i '.groups[].rules[].annotations.summary style="literal" | .groups[].rules[].annotations.description style="literal"' "$${f}" || exit 1; \
	done
