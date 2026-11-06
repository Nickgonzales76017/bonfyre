package control

// v1 owns six manual campaign dimensions. Additional URL parameters retain
// their original bytes and do not become native campaign coordinates.
#CampaignCoordinate: string & != "" & !~#"[\x00-\x1f\x7f\\]"#
#CampaignUTM: {
	utm_source?: #CampaignCoordinate
	utm_medium?: #CampaignCoordinate
	utm_campaign?: #CampaignCoordinate
	utm_id?: #CampaignCoordinate
	utm_term?: #CampaignCoordinate
	utm_content?: #CampaignCoordinate
}
#CampaignBuildRequest: {
	version: 1
	operation: "build"
	url: string & != ""
	utm: #CampaignUTM & {
		utm_source: #CampaignCoordinate
		utm_medium: #CampaignCoordinate
		utm_campaign: #CampaignCoordinate
	}
	_urlBytes: len(url) & <=8192
	for key, value in utm {
		_coordinateBytes: "\(key)": len(value) & <=256
	}
}
#CampaignReadRequest: {
	version: 1
	operation: "parse" | "validate"
	url: string & != ""
	_urlBytes: len(url) & <=8192
}
#CampaignLinkRequest: #CampaignBuildRequest | #CampaignReadRequest
#CampaignLinkResult: {
	schema: "bonfyre.campaign-link.v1"
	ok: true
	operation: "build" | "parse" | "validate"
	url: string & != ""
	tagging: "absent" | "partial" | "complete"
	utm: #CampaignUTM
	provenance: "authored_coordinates" | "untrusted_url_coordinates"
	external_effect_executed: false
	_urlBytes: len(url) & <=8192
	for key, value in utm {
		_coordinateBytes: "\(key)": len(value) & <=256
	}
	if operation == "build" {
		tagging: "complete"
		provenance: "authored_coordinates"
	}
	if operation != "build" { provenance: "untrusted_url_coordinates" }
	if tagging == "complete" {
		utm: {utm_source: #CampaignCoordinate, utm_medium: #CampaignCoordinate, utm_campaign: #CampaignCoordinate}
	}
	if tagging == "absent" { utm: close({}) }
	if tagging == "partial" {
		_present: len(utm) & >0
		_required: [for key, _ in utm if key == "utm_source" || key == "utm_medium" || key == "utm_campaign" {key}]
		_requiredCount: len(_required) & <3
	}
}
#CampaignLinkError: {
	schema: "bonfyre.campaign-link.v1"
	ok: false
	error: "invalid_command" | "input_read_failed" | "request_too_large" |
		"invalid_request" | "invalid_url" | "destination_not_public_dns" |
		"invalid_query" | "noncanonical_utm_key" | "duplicate_utm" |
		"invalid_utm_value" | "required_utm_missing" |
		"destination_already_tagged" | "generated_url_too_large"
}
campaignAttributionContract: {
	request_max_bytes: 16384
	url_max_utf8_bytes: 8192
	coordinate_max_utf8_bytes: 256
	coordinate_order: ["utm_source", "utm_medium", "utm_campaign", "utm_id", "utm_term", "utm_content"]
	workspace: {
		pack: "experience.source-campaign.workspace"
		entry: "campaigns/"
		route_suffix: "_campaign-link"
		get_permission: "read"
		post_permission: "write"
		identity_source: "current-native-auth-and-verified-workspace-binding"
		post_media: ["application/json", "application/x-www-form-urlencoded"]
		cache_control: "private, no-store"
		html_referrer_policy: "same-origin"
		storage_effect: false
		external_effect: false
	}
	destination_policy: "http-or-https-dns-name-no-credentials-no-ip-literals-no-local-suffixes"
	provenance_separation: "Campaign coordinates are independent of citation/source URL canonicalization."
	forbidden_inference: "A built or parsed link proves no arrival, identity, consent, lead, conversion, publication, dispatch, or reachable destination."
}
