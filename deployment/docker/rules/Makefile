
RULES_DIR ?= ./deployment/docker/rules

format-rules: install-yq
	find $(RULES_DIR) -type f -name "*.yml" | while IFS= read -r f; do \
		echo "Processing $${f}"; \
		yq -i '(.groups[].rules[] | select(.annotations.summary != null) | .annotations.summary) style="literal" | (.groups[].rules[] | select(.annotations.description != null) | .annotations.description) style="literal"' "$${f}" || exit 1; \
	done

install-yq:
	which yq || go install github.com/mikefarah/yq/v4@latest
