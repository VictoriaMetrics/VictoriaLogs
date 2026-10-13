
RULES_DIR ?= ./deployment/docker/rules

format-rules:
	find $(RULES_DIR) -type f -name "*.yml" | while IFS= read -r f; do \
		echo "Processing $${f}"; \
		yq -i '(.groups[].rules[] | select(.annotations.summary != null) | .annotations.summary) style="literal" | (.groups[].rules[] | select(.annotations.description != null) | .annotations.description) style="literal"' "$${f}" || exit 1; \
	done
