<?php
/**
 * Plugin Name: Aurekai Bridge
 * Description: Exposes a nonce-scoped, read-only installation-control proof for an Aurekai pairing ceremony.
 * Version: 0.1.0
 * License: GPL-2.0-or-later
 */
if (!defined('ABSPATH')) { exit; }

define('AUREKAI_BRIDGE_CLAIM_SCHEMA', 'bonfyre.wordpress.site-claim.v1');

function aurekai_bridge_secret() {
    $secret = get_option('aurekai_bridge_pairing_secret', '');
    if (!is_string($secret) || strlen($secret) < 32) {
        $secret = wp_generate_password(64, true, true);
        update_option('aurekai_bridge_pairing_secret', $secret, false);
    }
    return $secret;
}

function aurekai_bridge_origin() {
    $parts = wp_parse_url(home_url('/'));
    if (!$parts || empty($parts['host'])) { return ''; }
    return 'https://' . strtolower($parts['host']) . (!empty($parts['port']) ? ':' . intval($parts['port']) : '');
}

function aurekai_bridge_claim(WP_REST_Request $request) {
    $nonce = trim((string) $request->get_param('nonce'));
    $site = aurekai_bridge_origin();
    if ($site === '' || $nonce === '' || strlen($nonce) > 160 || !preg_match('/^[A-Za-z0-9._:-]+$/', $nonce)) {
        return new WP_Error('aurekai_invalid_claim', 'A valid claim nonce is required.', array('status' => 400));
    }
    $message = AUREKAI_BRIDGE_CLAIM_SCHEMA . "\n" . $site . "\n" . $nonce;
    return rest_ensure_response(array(
        'schema_version' => AUREKAI_BRIDGE_CLAIM_SCHEMA,
        'site' => $site,
        'nonce' => $nonce,
        'proof' => 'sha256=' . hash_hmac('sha256', $message, aurekai_bridge_secret()),
        'cannot_infer' => 'This proves control of this installed bridge for this nonce only; it does not prove a person, institution, publication, authority, OAuth grant, private data access, or publishing right.',
    ));
}

function aurekai_bridge_register_routes() {
    register_rest_route('aurekai-bridge/v1', '/claim', array(
        'methods' => WP_REST_Server::READABLE,
        'callback' => 'aurekai_bridge_claim',
        'permission_callback' => '__return_true',
        'args' => array('nonce' => array('required' => true, 'type' => 'string')),
    ));
}
add_action('rest_api_init', 'aurekai_bridge_register_routes');
