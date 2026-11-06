.PHONY: all test
all:
	@$(MAKE) -C ../BonfyreWebAddress bonfyre-web-address
test: all
	@bun test ../BonfyreWebAddress/tests/community_journey.test.mjs --test-name-pattern 'guild'
	@sh ../BonfyreWebAddress/tests/role_program_owner_test.sh ../BonfyreWebAddress/bonfyre-web-address
	@bun ../BonfyreWebAddress/tests/stitch_five_families_browser.test.mjs
	@bun ../BonfyreWebAddress/tests/stitch_next_depth_browser.test.mjs
	@sh ../BonfyreWebAddress/tests/account_world_spaces_test.sh
	@sh ../BonfyreWebAddress/tests/account_world_cinema_test.sh
	@sh ../BonfyreWebAddress/tests/source_push_operations_test.sh ../BonfyreWebAddress/bonfyre-web-address
	@sh ../BonfyreWebAddress/tests/page_saved_test.sh ../BonfyreWebAddress/bonfyre-web-address
	@$(MAKE) -C ../BonfyreWebAddress native-experience-composition-witness
	@cc -std=c11 -Wall -Wextra -Werror ../BonfyreWebAddress/tests/native_experience_composition_test.c -o /tmp/bonfyre-experience-test-$$$$ && \
	  /tmp/bonfyre-experience-test-$$$$ && rm -f /tmp/bonfyre-experience-test-$$$$
	@cc -std=c11 -Wall -Wextra -Werror ../BonfyreWebAddress/tests/stitch_work_presentation_test.c -o /tmp/bonfyre-stitch-work-test-$$$$ && \
	  /tmp/bonfyre-stitch-work-test-$$$$ && rm -f /tmp/bonfyre-stitch-work-test-$$$$
	@$(MAKE) -C ../BonfyreWebAddress people-projection-witness
	@bun test ../../integrations/browser/tests/source-catalogue.test.mjs
	@bun test ../../integrations/browser/tests/connection-studio-signin.browser.test.mjs --test-name-pattern 'Stitch native relay'
	@bun test ../../cmd/BonfyreWebAddress/tests/public_audio_deck_test.mjs --test-name-pattern 'Stitch audio queue'
	@bun test ../../cmd/BonfyreWebAddress/tests/profile_site_scope.test.mjs --test-name-pattern 'Stitch member feed'
	@sh ../BonfyreWebAddress/tests/page_composition_test.sh ../BonfyreWebAddress/bonfyre-web-address
	@bun test ../../scripts/tests/source-library-browser.test.mjs --test-name-pattern 'Stitch'
	@bun ../BonfyreWebAddress/tests/organization_browser_test.mjs
	@BONFYRE_STITCH_BROWSER=1 $(MAKE) -C ../BonfyreWebAddress page-events-witness
	@node ../BonfyreWebAddress/tests/stitch_work_routes_test.mjs

.PHONY: browser-depth
browser-depth:
	@bun ../BonfyreWebAddress/tests/stitch_five_families_browser.test.mjs
	@bun ../BonfyreWebAddress/tests/stitch_next_depth_browser.test.mjs
