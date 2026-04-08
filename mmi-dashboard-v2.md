---
sqlpage-conf:
  database_url: "sqlite://resource-surveillance.sqlite.db?mode=rwc"
  web_root: "./dev-src.auto"
  allow_exec: true
  port: 9227
---
# Medigy Market Intelligence — Unified SQLPage Application (v3)

This application is built on the **unified extensible pipeline** (`medigy-unified-v2.sql`).

**Architecture principle:** Adding a new disease condition requires inserting
one row into `dim_condition_registry`. The landing page, drilldown pages,
and all analytics update automatically — no SQL or page logic changes required.

**v3 changes:**

- All `big_number` components replaced with navigable `card` components
- Slow analytics views materialized as indexed tables (`mat_*`)
- External CSS (`custom-dashboard.css`) applied across all pages
- New visualizations: opportunity scatter, tier distribution donut,
  source-mix bar, top-state treemap equivalent, data freshness timeline
- Same-page anchor navigation via HTML anchors + card links

---

```bash prepare-db-deploy-server --descr "Ingest raw files, build unified analytics, launch SQLPage UI."
#!/bin/bash
set -euo pipefail

rm -f resource-surveillance.sqlite.*

surveilr ingest files -r medicare-ds/ 
surveilr orchestrate transform-csv
surveilr shell sql/medigy-ddl.sql
surveilr shell sql/medigy-unified-v2.sql
surveilr shell sql/medigy-materialized.sql
spry sp spc --package --conf sqlpage/sqlpage.json -m mmi-dashboard.md | sqlite3 resource-surveillance.sqlite.db
echo "Medigy Market Intelligence (v3) is ready."
```

---

## Global Shell Partial

```sql PARTIAL global-layout.sql --inject *.sql --inject mmi/*.sql

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;
```

---

## Registration Page

```sql index.sql { route: { caption: "Registration" } }
-- @route.description "User registration gate before entering the dashboard"

SELECT 'cookie' AS component,
       'isVerified' AS name,
       'false' AS value,
       '/' AS path,
       'lax' AS same_site
WHERE COALESCE(sqlpage.cookie('isVerified'), '') = '';

SELECT 'shell' AS component,
       'Medigy Market Intelligence — Registration' AS title,
       NULL AS icon,
       'narrow' AS layout,
       true AS fixed_top_menu,
       './' AS link,
    './footer-links.js' AS javascript,
       '© 2026 Medigy Market Intelligence' AS footer;

SELECT 'hero' AS component,
       'Welcome! Let’s Get You Started' AS title,
       'To provide you with a better experience and keep you updated, we’d like to collect a few basic details.
This will only take a few seconds.' AS description,
       'azure' AS color;

SELECT 'alert' AS component,
       'Invalid Email Address' AS title,
       'The email address you entered is not valid. Please enter a valid email address (e.g. user@example.com).' AS description,
       'danger' AS color
WHERE $error = 'invalid_email';

SELECT 'alert' AS component,
       'Invalid Phone Number' AS title,
    'Please enter a valid phone number with country code (e.g. +14155552671).' AS description,
       'danger' AS color
WHERE $error = 'invalid_phone';

SELECT 'alert' AS component,
       'Consent Required' AS title,
       'Please review the consent text and confirm your agreement before continuing.' AS description,
       'danger' AS color
WHERE $error = 'invalid_consent';

SELECT 'form' AS component, 'Get' AS method, 'Continue to Application' AS validate;

SELECT
    'first_name' AS name,
    'First Name' AS label,
    'text' AS type,
    true AS required;

SELECT
    'second_name' AS name,
    'Second Name' AS label,
    'text' AS type,
    true AS required;

SELECT
    'email_address' AS name,
    'Email Address' AS label,
    'email' AS type,
    true AS required;

SELECT
    'phone_number' AS name,
    'Phone Number (with country code)' AS label,
    'tel' AS type,
    COALESCE(NULLIF($phone_number, ''), '') AS value,
    false AS required;

SELECT
    'organization' AS name,
    'Organization' AS label,
    'text' AS type,
    false AS required;

SELECT
    'purpose_of_visit' AS name,
    'Purpose of Visit' AS label,
    'select' AS type,
    COALESCE(NULLIF($purpose_of_visit, ''), '') AS value,
    '[{"value":"","label":"Select purpose (optional)"},{"value":"Exploring features","label":"Exploring features"},{"value":"Research / Study","label":"Research / Study"},{"value":"Business / Professional use","label":"Business / Professional use"},{"value":"Other","label":"Other"}]' AS options;

SELECT
    'consent_acknowledged' AS name,
    'I agree to the consent and compliance statement' AS label,
    'checkbox' AS type,
    COALESCE(NULLIF($consent_acknowledged, ''), 'yes') AS value,
    'By continuing, you agree that we may use your contact information to communicate updates, product information, and relevant notifications. We respect your privacy and will not share your data with third parties.' AS description,
    true AS required;

SELECT 'html' AS component,
    '<p style="text-align:center; margin-top:8px;">Your information is safe and will be handled securely.</p>' AS html;

```

---

## Registration Submit Handler

```sql registration-submit.sql { route: { caption: "Registration Submit Handler" } }
-- @route.description "Sends a welcome email after registration submit using SQLPage exec + SMTP, then redirects"

SET registration_profile_cookie = sqlpage.cookie('medigy_mmi_registration_profile_v2');
SET smtp_host = COALESCE(NULLIF(TRIM(sqlpage.environment_variable('EMAIL_HOST')), ''), '');
SET smtp_port = COALESCE(NULLIF(TRIM(sqlpage.environment_variable('EMAIL_PORT')), ''), '');
SET smtp_username = COALESCE(NULLIF(TRIM(sqlpage.environment_variable('EMAIL_USERNAME')), ''), '');
SET smtp_password = COALESCE(NULLIF(TRIM(sqlpage.environment_variable('EMAIL_APP_PASSWORD')), ''), '');
SET smtp_from = COALESCE(NULLIF(TRIM(sqlpage.environment_variable('EMAIL_FROM')), ''), '');
SET recipient_email = COALESCE(NULLIF(TRIM(sqlpage.environment_variable('RECEIVER_EMAIL')), ''), '');
SET submitted_first_name = COALESCE(NULLIF($first_name, ''), '');
SET submitted_second_name = COALESCE(NULLIF($second_name, ''), '');
SET submitted_email_address = COALESCE(NULLIF($email_address, ''), '');
SET submitted_phone_number = TRIM(COALESCE(NULLIF(NULLIF(TRIM($phone_number), ''), '+1'), ''));
SET submitted_phone_number_sanitized = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($submitted_phone_number, '+', ''), ' ', ''), '-', ''), '(', ''), ')', ''), '.', '');
SET submitted_phone_digits_stripped = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($submitted_phone_number_sanitized,'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9','');
SET submitted_organization = COALESCE(NULLIF($organization, ''), '');
SET submitted_purpose_of_visit = COALESCE(NULLIF($purpose_of_visit, ''), '');
SET submitted_consent_acknowledged = LOWER(TRIM(COALESCE(NULLIF($consent_acknowledged, ''), '')));
SET submitted_full_name = TRIM($submitted_first_name || ' ' || $submitted_second_name);
SET submitted_access_timestamp = STRFTIME('%Y-%m-%d %H:%M:%S UTC', 'now');
SET submitted_ip_address = COALESCE(
    NULLIF(TRIM($ip_address), ''),
    NULLIF(TRIM(sqlpage.exec('sh', '-c', 'hostname -I 2>/dev/null | awk ''{print $1}'' | tr -d ''[:space:]''')), ''),
    'N/A'
);
SET submitted_user_agent = COALESCE(NULLIF(TRIM($user_agent), ''), 'N/A');

-- Email validation: each condition multiplied — SQLite booleans return 1/0 so product = 1 only when all pass
SET email_is_valid =
    (TRIM(COALESCE($submitted_email_address, '')) != '') *
    (LENGTH($submitted_email_address) - LENGTH(REPLACE($submitted_email_address, '@', '')) = 1) *
    (INSTR($submitted_email_address, '@') > 1) *
    (INSTR(SUBSTR($submitted_email_address, INSTR($submitted_email_address, '@') + 1), '.') > 0) *
    (SUBSTR($submitted_email_address, LENGTH($submitted_email_address), 1) != '.');

-- Phone validation: optional — passes when empty; if provided must start with +, digits-only body, length 6-15
SET phone_is_valid = CASE
    WHEN TRIM(COALESCE($submitted_phone_number, '')) = '' THEN 1
    ELSE
        (SUBSTR($submitted_phone_number, 1, 1) = '+') *
        (LENGTH($submitted_phone_number_sanitized) >= 6) *
        (LENGTH($submitted_phone_number_sanitized) <= 15) *
        ($submitted_phone_digits_stripped = '')
    END;

SET consent_is_valid = ($submitted_consent_acknowledged IN ('yes', 'on', 'true', '1'));

-- Redirect back to registration form with error if email is invalid
SELECT 'redirect' AS component,
       '/?error=invalid_email' AS link
WHERE $email_is_valid = 0;

SELECT 'redirect' AS component,
       '/?error=invalid_phone' AS link
WHERE $email_is_valid = 1 AND $phone_is_valid = 0;

SELECT 'redirect' AS component,
       '/?error=invalid_consent' AS link
WHERE $email_is_valid = 1 AND $phone_is_valid = 1 AND $consent_is_valid = 0;

SET smtp_exec_command =
    'RECIP="' || $recipient_email || '"; ' ||
    'if [ -z "$RECIP" ]; then echo SKIPPED_NO_EMAIL_RECIPIENT; echo CURL_EXIT_CODE:0; exit 0; fi; ' ||
    'if [ -z "' || $smtp_host || '" ] || [ -z "' || $smtp_port || '" ] || [ -z "' || $smtp_username || '" ] || [ -z "' || $smtp_password || '" ] || [ -z "' || $smtp_from || '" ]; then echo SKIPPED_MISSING_SMTP_CONFIG; echo CURL_EXIT_CODE:0; exit 0; fi; ' ||
    'MESSAGE="From: ' || $smtp_from ||
    '\r\nTo: ' || $recipient_email ||
    '\r\nSubject: New User Access Notification – Medigy Market Intelligence (MMI) Application Entry' ||
    '\r\nMIME-Version: 1.0' ||
    '\r\nContent-Type: text/html; charset=UTF-8' ||
    '\r\n\r\n<html><body>' ||
    '<p>Hi Team,</p>' ||
    '<p>A new user has accessed Medigy Market Intelligence (MMI) Application and submitted their details.</p>' ||
    '<p><strong>User Information:</strong></p>' ||
    '<ul>' ||
    '<li><strong>Full Name:</strong> ' || REPLACE($submitted_full_name, '"', '''') || '</li>' ||
    '<li><strong>Email Address:</strong> ' || REPLACE($submitted_email_address, '"', '''') || '</li>' ||
    '<li><strong>Phone Number:</strong> ' || REPLACE($submitted_phone_number, '"', '''') || '</li>' ||
    '<li><strong>Organization / Company:</strong> ' || REPLACE($submitted_organization, '"', '''') || '</li>' ||
    '<li><strong>Purpose of Visit:</strong> ' || REPLACE($submitted_purpose_of_visit, '"', '''') || '</li>' ||
    '</ul>' ||
    '<p><strong>Access Details:</strong></p>' ||
    '<ul>' ||
    '<li><strong>Date &amp; Time:</strong> ' || REPLACE($submitted_access_timestamp, '"', '''') || '</li>' ||
    '<li><strong>IP Address:</strong> ' || REPLACE($submitted_ip_address, '"', '''') || '</li>' ||
    '<li><strong>Device / Browser:</strong> ' || REPLACE($submitted_user_agent, '"', '''') || '</li>' ||
    '</ul>' ||
    '<p><strong>Notes:</strong><br>This notification is generated automatically when a user enters their details on the MMI Application entry screen. The information can be used for follow-up communication, support, or engagement purposes.</p>' ||
    '<p>Please reach out to the user if required.</p>' ||
    '<p>Regards,<br>System Notification</p>' ||
    '</body></html>' ||
    '\r\n"; ' ||
    'printf "%b" "$MESSAGE" > /tmp/smtp_msg_$$.txt; ' ||
    'CURLOUT=$(curl --no-progress-meter --verbose --url "smtp://' || $smtp_host || ':' || $smtp_port || '" --ssl-reqd --user "' || $smtp_username || ':' || $smtp_password || '" --mail-from "' || $smtp_from || '" --mail-rcpt "' || $recipient_email || '" --upload-file /tmp/smtp_msg_$$.txt 2>&1); ' ||
    'CURL_EXIT_CODE=$?; rm -f /tmp/smtp_msg_$$.txt; ' ||
    'echo "$CURLOUT" | grep -E "^[<*] " | tr -d "\r"; ' ||
    'echo CURL_EXIT_CODE:$CURL_EXIT_CODE; exit 0';

SET smtp_exec_response = sqlpage.exec(
    'sh',
    '-c',
    $smtp_exec_command
);

SET email_send_status = CASE
    WHEN INSTR(COALESCE($smtp_exec_response, ''), 'SKIPPED_NO_EMAIL_RECIPIENT') > 0 THEN 'SKIPPED_NO_EMAIL'
    WHEN INSTR(COALESCE($smtp_exec_response, ''), 'SKIPPED_MISSING_SMTP_CONFIG') > 0 THEN 'SKIPPED_MISSING_SMTP_CONFIG'
    WHEN INSTR(COALESCE($smtp_exec_response, ''), 'CURL_EXIT_CODE:0') > 0
         AND INSTR(COALESCE($smtp_exec_response, ''), '< 250') > 0 THEN 'SUCCESS'
    WHEN INSTR(COALESCE($smtp_exec_response, ''), 'CURL_EXIT_CODE:0') > 0 THEN 'SENT_UNCONFIRMED'
    ELSE 'FAILED'
END;

SET email_log_line =
    '--- ' || STRFTIME('%Y-%m-%dT%H:%M:%SZ', 'now') || ' ---' ||
    '\nstatus         : ' || $email_send_status ||
    '\nto             : ' || $recipient_email ||
    '\nfrom           : ' || $smtp_from ||
    '\nsmtp_host      : ' || $smtp_host ||
    '\nsmtp_port      : ' || $smtp_port ||
    '\nsmtp_username  : ' || $smtp_username ||
    '\ncookie         : isVerified=' || CASE WHEN $email_send_status = 'SUCCESS' THEN 'true' ELSE 'false' END || '; path=/; samesite=lax' ||
    '\nprofile_cookie : ' || COALESCE(REPLACE($registration_profile_cookie, '"', ''''), 'not-set') ||
    '\nsmtp_log       : ' || REPLACE(REPLACE(COALESCE($smtp_exec_response, 'none'), CHAR(13), ''), CHAR(10), ' | ');

SET email_log_append_status = sqlpage.exec(
    'sh',
    '-c',
    'printf "%b\n\n" "' || $email_log_line || '" >> ' || sqlpage.current_working_directory() || '/sqlpage/email_send_status.txt; echo LOG_APPEND_OK'
);

SELECT 'cookie' AS component,
       'isVerified' AS name,
       CASE WHEN $email_send_status = 'SUCCESS' THEN 'true' ELSE 'false' END AS value,
       '/' AS path,
       'lax' AS same_site
WHERE $email_is_valid = 1 AND $phone_is_valid = 1 AND $consent_is_valid = 1;

SELECT 'redirect' AS component,
       '/mmi/home-overview.sql' AS link
WHERE $email_is_valid = 1 AND $phone_is_valid = 1 AND $consent_is_valid = 1;
```

```contribute sqlpage_files --base .
./footer-links.js .
./custom-dashboard.css .
```

---

## Home Page

```sql registration.sql { route: { caption: "Registration Alias" } }
-- @route.description "Alias route for user registration gate"

SELECT 'cookie' AS component,
       'isVerified' AS name,
       'false' AS value,
       '/' AS path,
       'lax' AS same_site
WHERE COALESCE(sqlpage.cookie('isVerified'), '') = '';

SELECT 'shell' AS component,
       'Medigy Market Intelligence — Registration' AS title,
       NULL AS icon,
       'narrow' AS layout,
       true AS fixed_top_menu,
       './' AS link,
    './footer-links.js' AS javascript,
       '© 2026 Medigy Market Intelligence' AS footer;

SELECT 'hero' AS component,
       'Welcome! Let’s Get You Started' AS title,
       'To provide you with a better experience and keep you updated, we’d like to collect a few basic details. This will only take a few seconds.' AS description,
       'azure' AS color;

SELECT 'alert' AS component,
       'Invalid Email Address' AS title,
       'The email address you entered is not valid. Please enter a valid email address (e.g. user@example.com).' AS description,
       'danger' AS color
WHERE $error = 'invalid_email';

SELECT 'alert' AS component,
       'Invalid Phone Number' AS title,
    'Please enter a valid phone number with country code (e.g. +14155552671).' AS description,
       'danger' AS color
WHERE $error = 'invalid_phone';

SELECT 'alert' AS component,
       'Consent Required' AS title,
       'Please review the consent text and confirm your agreement before continuing.' AS description,
       'danger' AS color
WHERE $error = 'invalid_consent';

SELECT 'form' AS component, 'Get' AS method, 'Continue to Application' AS validate;

SELECT
    'first_name' AS name,
    'First Name' AS label,
    'text' AS type,
    true AS required;

SELECT
    'second_name' AS name,
    'Second Name' AS label,
    'text' AS type,
    true AS required;

SELECT
    'email_address' AS name,
    'Email Address' AS label,
    'email' AS type,
    true AS required;

SELECT
    'phone_number' AS name,
    'Phone Number (with country code)' AS label,
    'tel' AS type,
    COALESCE(NULLIF($phone_number, ''), '') AS value,
    false AS required;

SELECT
    'organization' AS name,
    'Organization' AS label,
    'text' AS type,
    false AS required;

SELECT
    'purpose_of_visit' AS name,
    'Purpose of Visit' AS label,
    'select' AS type,
    COALESCE(NULLIF($purpose_of_visit, ''), '') AS value,
    '[{"value":"","label":"Select purpose (optional)"},{"value":"Exploring features","label":"Exploring features"},{"value":"Research / Study","label":"Research / Study"},{"value":"Business / Professional use","label":"Business / Professional use"},{"value":"Other","label":"Other"}]' AS options;

SELECT
    'consent_acknowledged' AS name,
    'I agree to the consent and compliance statement' AS label,
    'checkbox' AS type,
    COALESCE(NULLIF($consent_acknowledged, ''), 'yes') AS value,
    'By continuing, you agree that we may use your contact information to communicate updates, product information, and relevant notifications. We respect your privacy and will not share your data with third parties.' AS description,
    true AS required;

SELECT 'html' AS component,
    '<p style="text-align:center; margin-top:8px;">Your information is safe and will be handled securely.</p>' AS html;

```


---

## Overview Page

```sql mmi/home-overview.sql { route: { caption: "Overview" } }
-- @route.description "MMI landing page — portfolio KPIs, condition cards, navigation"

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

-- Hero
SELECT 'hero' AS component,
       'Medigy Market Intelligence' AS title,
       'Unified CMS Medicare analytics across all disease conditions — Part B, DMEPOS, and Hospital data.' AS description,
       'azure' AS color;

-- ── Portfolio Totals — Navigable KPI Cards (replaces big_number) ──────────────
-- Cards link to relevant sections/pages; same-page anchors via HTML id elements.
SELECT 'card' AS component, 4 AS columns;

SELECT 
    'Disease Conditions' AS title,
    printf('%,.0f', total_conditions) AS description,    
    'teal' AS color, 'activity' AS icon,
    'kpi-card' AS class,
    '#disease-conditions-section' AS link
FROM mat_executive_kpis;

SELECT 
    'Total Beneficiaries' AS title,
    printf('%,.0f', total_beneficiaries) AS description,
    'azure' AS color, 'users' AS icon,
    'kpi-card' AS class,
    '/mmi/executive-dashboard.sql' AS link
FROM mat_executive_kpis;

SELECT 
    'Medicare Allowed ($)' AS title,
    '$' || printf('%,.0f', total_allowed_amt) AS description,
    'indigo' AS color, 'currency-dollar' AS icon,
    'kpi-card' AS class,
    '/mmi/executive-dashboard.sql' AS link
FROM mat_executive_kpis;

SELECT 
    'External Data Sources' AS title,
    '' || (SELECT COUNT(*) FROM data_provenance WHERE object_type = 'external_source') || '' AS description,
    'orange' AS color, 'database' AS icon,
    'kpi-card' AS class,
    '/mmi/data-dictionary.sql' AS link;

-- Second row — additional KPIs with same-page navigation
SELECT 'card' AS component, 4 AS columns;

SELECT 
    'Active States' AS title,
    printf('%,.0f', total_states) AS description,
    'blue' AS color, 'map-pin' AS icon,
    'kpi-card' AS class,
    '/mmi/geography.sql' AS link
FROM mat_executive_kpis;

SELECT 
    'Unique Procedures' AS title,
    printf('%,.0f', total_procedures) AS description,
    'grape' AS color, 'clipboard-list' AS icon,
    'kpi-card' AS class,
    '/mmi/procedure-drilldown.sql' AS link
FROM mat_executive_kpis;

SELECT 
    'Medicare Paid ($)' AS title,
    '$' || printf('%,.0f', total_medicare_payment) AS description,
    'teal' AS color, 'receipt' AS icon,
    'kpi-card' AS class,
    '/mmi/executive-dashboard.sql#payment-section' AS link
FROM mat_executive_kpis;

SELECT 
    'Opportunity Scores' AS title,
    (SELECT COUNT(*) FROM mat_opportunity_score) || ' ranked' AS description,
    'orange' AS color, 'trophy' AS icon,
    'kpi-card' AS class,
    '/mmi/opportunity-scoring.sql' AS link;

-- ── NEW: Opportunity Snapshot Chart ──────────────────────────────────────────
SELECT 'divider' AS component, 'Market Opportunity Snapshot' AS label;

SELECT 'chart' AS component,
       'Opportunity Score by Condition' AS title,
       'bar' AS type, 8 AS width;
SELECT condition_name AS label, opportunity_score AS value, color AS color
FROM mat_opportunity_score
ORDER BY opportunity_score DESC;

-- ── NEW: Portfolio Mix by Tier (donut) ───────────────────────────────────────
SELECT 'chart' AS component,
       'Portfolio Distribution by Tier' AS title,
       'donut' AS type, 4 AS width;
SELECT 
    'Tier ' || tier || ' — ' || CASE tier WHEN 1 THEN 'Flagship' WHEN 2 THEN 'Core' ELSE 'Baseline' END AS label,
    SUM(total_allowed_amt) AS value
FROM mat_condition_national_summary
GROUP BY tier
ORDER BY tier;

-- ── NEW: Data Source Mix (bar) ────────────────────────────────────────────────
SELECT 'chart' AS component,
       'Medicare Allowed by Data Source (All Conditions)' AS title,
       'bar' AS type, 4 AS width;
SELECT source_type AS label, SUM(total_allowed_amt) AS value
FROM mat_condition_source_breakdown
GROUP BY source_type
ORDER BY SUM(total_allowed_amt) DESC;

-- ── Disease Condition Cards — Fully Dynamic ───────────────────────────────────
SELECT 'html' AS component, '<div id="disease-conditions-section"></div>' AS html;
SELECT 'divider' AS component, 'Disease Conditions — Click to Explore' AS label;
SELECT 'card' AS component, 3 AS columns;

SELECT
    s.condition_name                                           AS title,
    s.specialty_domain || ' | Tier ' || s.tier                AS subtitle,
    '$' || printf('%,.0f', s.total_allowed_amt)
        || ' Medicare Allowed | '
        || printf('%,.0f', s.total_beneficiaries)
        || ' beneficiaries'                                    AS description,
    '/mmi/condition-hub.sql?condition=' || REPLACE(s.condition_name, ' ', '%20') AS link,
    s.icon                                                     AS icon,
    s.color                                                    AS color
FROM mat_condition_national_summary s
ORDER BY s.tier, s.opportunity_score DESC;

-- ── Analytics Navigation ──────────────────────────────────────────────────────
SELECT 'divider' AS component, 'Analytics Modules' AS label;
SELECT 'card' AS component, 3 AS columns;

SELECT 'Executive Dashboard'  AS title,
       'Top-line KPIs across all conditions and specialties.' AS description,
       '/mmi/executive-dashboard.sql' AS link,
       'layout-dashboard' AS icon, 'teal' AS color;
SELECT 'Opportunity Scoring'  AS title,
       'Composite ranking of all disease conditions by patient volume and spend.' AS description,
       '/mmi/opportunity-scoring.sql' AS link,
       'trophy' AS icon, 'azure' AS color;
SELECT 'Geographic Intelligence' AS title,
       'State-level market sizing and cost-tier mapping.' AS description,
       '/mmi/geography.sql' AS link,
       'map-pin' AS icon, 'indigo' AS color;
SELECT 'Procedure Drilldown'  AS title,
       'HCPCS-level analytics across all conditions.' AS description,
       '/mmi/procedure-drilldown.sql' AS link,
       'clipboard-list' AS icon, 'orange' AS color;
SELECT 'Disease Conditions'   AS title,
       'Full directory of conditions in the registry.' AS description,
       '/mmi/conditions.sql' AS link,
       'virus' AS icon, 'grape' AS color;
SELECT 'Data Dictionary'      AS title,
       'Table and column reference for the unified pipeline.' AS description,
       '/mmi/data-dictionary.sql' AS link,
       'book' AS icon, 'blue' AS color;
```

---

## Condition Hub — Universal Disease Drilldown Page

```sql mmi/condition-hub.sql { route: { caption: "Condition Hub" } }

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

-- Hero — dynamic per condition
SELECT 'hero' AS component,
       s.condition_name AS title,
       s.specialty_domain || ' — ' || s.b2b_tier_primary
           || ' | Tier ' || s.tier
           || ' | ' || s.data_sources || ' data source(s)' AS description,
       s.color AS color
FROM mat_condition_national_summary s
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
LIMIT 1;

-- ── National KPI Strip — Navigable Cards (replaces big_number) ────────────────
-- Anchor links allow same-page navigation to tables below
SELECT 'html' AS component, '<div id="national-kpi-section"></div>' AS html;
SELECT 'divider' AS component, 'National Summary' AS label;

SELECT 'card' AS component, 4 AS columns;

SELECT 'Total Beneficiaries' AS title,
       printf('%,.0f', total_beneficiaries) AS description,
       color AS color, 'users' AS icon,
       'kpi-hub-card' AS class,
       '#geo-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Total Services' AS title,
       printf('%,.0f', total_services) AS description,
       color AS color, 'activity' AS icon,
       'kpi-hub-card' AS class,
       '#procedures-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Medicare Allowed' AS title,
       '$' || printf('%,.0f', total_allowed_amt) AS description,
       color AS color, 'currency-dollar' AS icon,
       'kpi-hub-card' AS class,
       '#source-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Allowed / Patient' AS title,
       '$' || printf('%,.2f', allowed_per_patient) AS description,
       color AS color, 'calculator' AS icon,
       'kpi-hub-card' AS class,
       '#national-kpi-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

-- Second KPI row — services per patient + states coverage
SELECT 'card' AS component, 4 AS columns;

SELECT 'Services / Patient' AS title,
       printf('%,.2f', services_per_patient) AS description,
       color AS color, 'heart-rate-monitor' AS icon,
       'kpi-hub-card' AS class,
       '#procedures-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'States with Data' AS title,
       printf('%,.0f', states_with_data) AS description,
       color AS color, 'map-pin' AS icon,
       'kpi-hub-card' AS class,
       '#geo-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Opportunity Score' AS title,
       ROUND(opportunity_score, 1) || ' / 100' AS description,
       color AS color, 'trophy' AS icon,
       'kpi-hub-card' AS class,
       '/mmi/opportunity-scoring.sql' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Medicare Paid ($)' AS title,
       '$' || printf('%,.0f', total_medicare_payment) AS description,
       color AS color, 'receipt' AS icon,
       'kpi-hub-card' AS class,
       '#source-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

-- ── Data Source Breakdown ─────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="source-breakdown-section"></div>' AS html;
SELECT 'divider' AS component, 'Data Source Layer Breakdown' AS label;

SELECT 'chart' AS component,
       'Medicare Allowed by Data Source' AS title,
       'donut' AS type, 5 AS width;
SELECT source_type AS label, total_allowed_amt AS value
FROM mat_condition_source_breakdown
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_allowed_amt DESC;

-- NEW: Beneficiary vs Spend comparison bar (same 7-col slot)
SELECT 'chart' AS component,
       'Beneficiaries by Data Source' AS title,
       'bar' AS type, 7 AS width;
SELECT source_type AS label, total_beneficiaries AS value
FROM mat_condition_source_breakdown
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_beneficiaries DESC;

-- ── Top Procedures ────────────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="procedures-section"></div>' AS html;

SET max_per_page = 10;
SET current_page = COALESCE(CAST($page AS INT), 1);
SET total_rows = (
    SELECT COUNT(*) FROM mat_condition_hcpcs_detail 
    WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
);
SET total_pages = ($total_rows + $max_per_page - 1) / $max_per_page;

SELECT 'divider' AS component, 'Top Procedures (HCPCS)' AS label;

SELECT 'table' AS component, 
       'Procedure-Level Analytics' AS title, 
       TRUE AS sort, TRUE AS search,
       'HCPCS' AS markdown;

SELECT
    hcpcs_code  AS "HCPCS",
    COALESCE(procedure_description, 'DRG ' || hcpcs_code) AS "Description",
    procedure_category                                   AS "Category",
    source_type                                          AS "Source",
    printf('%,.0f', total_services)                      AS "Services",
    printf('%,.0f', total_beneficiaries)                 AS "Beneficiaries",
    '$' || printf('%,.0f', total_allowed_amt)            AS "Total Allowed",
    '$' || printf('%,.2f', avg_allowed_per_service)      AS "$/Service"
FROM mat_condition_hcpcs_detail
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_allowed_amt DESC
LIMIT $max_per_page
OFFSET ($current_page - 1) * $max_per_page;

SELECT 'pagination' AS component,
    ($current_page <= 1) AS previous_disabled,
    ($current_page >= $total_pages) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page', $current_page - 1, 'condition', $condition, 'geo_page', $geo_page)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page', $current_page + 1, 'condition', $condition, 'geo_page', $geo_page)) AS next_link;

WITH RECURSIVE page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM page_numbers WHERE n < $total_pages
)
SELECT n AS contents,
       sqlpage.link(sqlpage.path(), json_object('page', n, 'condition', $condition, 'geo_page', $geo_page)) AS link,
       (n = $current_page) AS active FROM page_numbers;

-- ── Geographic Breakdown ──────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="geo-breakdown-section"></div>' AS html;

SET geo_max_per_page = 10;
SET geo_current_page = COALESCE(CAST($geo_page AS INT), 1);
SET geo_total_rows = (
    SELECT COUNT(*) FROM mat_condition_state_breakdown 
    WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
);
SET geo_total_pages = ($geo_total_rows + $geo_max_per_page - 1) / $geo_max_per_page;

SELECT 'divider' AS component, 'Geographic Breakdown' AS label;

-- NEW: Top states bar chart
SELECT 'chart' AS component,
       'Top 10 States by Medicare Allowed Spend' AS title,
       'bar' AS type, 12 AS width;
SELECT state_abbr AS label, total_allowed_amt AS value
FROM mat_condition_state_breakdown
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_allowed_amt DESC
LIMIT 10;

SELECT 'table' AS component, 
       'State-Level Market Analysis' AS title, 
       TRUE AS sort, TRUE AS search,
       'State' AS markdown;

SELECT
    state_abbr AS "State",
    CASE WHEN locality_name IS NULL OR TRIM(locality_name) = '' THEN 'Statewide' ELSE locality_name END AS "Locality",
    COALESCE(cost_tier, 'Unknown') AS "Cost Tier",
    printf('%,.0f', total_beneficiaries) AS "Beneficiaries",
    '$' || printf('%,.0f', total_allowed_amt) AS "Medicare Allowed",
    '$' || printf('%,.2f', allowed_per_patient) AS "Allowed/Patient"
FROM mat_condition_state_breakdown
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_allowed_amt DESC
LIMIT $geo_max_per_page
OFFSET ($geo_current_page - 1) * $geo_max_per_page;

SELECT 'pagination' AS component,
    ($geo_current_page <= 1) AS previous_disabled,
    ($geo_current_page >= $geo_total_pages) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('geo_page', $geo_current_page - 1, 'page', $page, 'condition', $condition)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('geo_page', $geo_current_page + 1, 'page', $page, 'condition', $condition)) AS next_link;

WITH RECURSIVE geo_page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM geo_page_numbers WHERE n < $geo_total_pages
)
SELECT n AS contents,
       sqlpage.link(sqlpage.path(), json_object('geo_page', n, 'page', $page, 'condition', $condition)) AS link,
       (n = $geo_current_page) AS active FROM geo_page_numbers;
```

---

## Executive Dashboard

```sql mmi/executive-dashboard.sql { route: { caption: "Executive Dashboard" } }

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
       'Executive Dashboard' AS title,
       'Cross-condition market intelligence — all diseases, all data sources, one view.' AS description,
       'azure' AS color;

-- ── Top-Line KPI Cards (replaces big_number) ─────────────────────────────────
-- Row 1: Core portfolio metrics with navigation to relevant sections/pages
SELECT 'html' AS component, '<div id="kpi-top"></div>' AS html;
SELECT 'divider' AS component, 'Portfolio Totals' AS label;
SELECT 'card' AS component, 3 AS columns;

SELECT 'Active Conditions' AS title, 
       total_conditions AS description,
       'teal' AS color, 'activity' AS icon,
       'kpi-card' AS class,
       '#condition-table-section' AS link
FROM mat_executive_kpis;

SELECT 'Total Beneficiaries' AS title, 
       printf('%,.0f', total_beneficiaries) AS description,
       'azure' AS color, 'users' AS icon,
       'kpi-card' AS class,
       '#condition-table-section' AS link
FROM mat_executive_kpis;

SELECT 'Medicare Allowed ($)' AS title, 
       '$' || printf('%,.0f', total_allowed_amt) AS description,
       'indigo' AS color, 'currency-dollar' AS icon,
       'kpi-card' AS class,
       '#condition-table-section' AS link
FROM mat_executive_kpis;

-- Row 2: Operational metrics
SELECT 'card' AS component, 3 AS columns;

SELECT 'Medicare Payments ($)' AS title, 
       '$' || printf('%,.0f', total_medicare_payment) AS description,
       'teal' AS color, 'receipt' AS icon,
       'kpi-card' AS class,
       '#payment-section' AS link
FROM mat_executive_kpis;

SELECT 'States with Data' AS title, 
       total_states AS description,
       'blue' AS color, 'map-pin' AS icon,
       'kpi-card' AS class,
       '/mmi/geography.sql' AS link
FROM mat_executive_kpis;

SELECT 'Data Sources Active' AS title, 
       active_data_sources AS description,
       'orange' AS color, 'database' AS icon,
       'kpi-card' AS class,
       '/mmi/data-dictionary.sql' AS link
FROM mat_executive_kpis;

-- ── Condition Comparison Charts ────────────────────────────────────────────────
SELECT 'divider' AS component, 'Condition Comparison' AS label;

SELECT 'chart' AS component,
       'Medicare Allowed by Condition ($)' AS title,
       'bar' AS type, 6 AS width;
SELECT condition_name AS label, total_allowed_amt AS value, color AS color
FROM mat_condition_national_summary
ORDER BY total_allowed_amt DESC;

SELECT 'chart' AS component,
       'Beneficiary Reach by Condition' AS title,
       'bar' AS type, 6 AS width;
SELECT condition_name AS label, total_beneficiaries AS value, color AS color
FROM mat_condition_national_summary
ORDER BY total_beneficiaries DESC;

SELECT 'chart' AS component,
       'Services per Patient by Condition' AS title,
       'bar' AS type, 6 AS width;
SELECT condition_name AS label, services_per_patient AS value, color AS color
FROM mat_condition_national_summary
ORDER BY services_per_patient DESC;

SELECT 'chart' AS component,
       'Allowed per Patient by Condition ($)' AS title,
       'bar' AS type, 6 AS width;
SELECT condition_name AS label, allowed_per_patient AS value, color AS color
FROM mat_condition_national_summary
ORDER BY allowed_per_patient DESC;

-- ── NEW: Payment vs Allowed ratio chart ───────────────────────────────────────
SELECT 'html' AS component, '<div id="payment-section"></div>' AS html;
SELECT 'divider' AS component, 'Payment Efficiency Analysis' AS label;

SELECT 'chart' AS component,
       'Medicare Payment Ratio by Condition (Payment ÷ Allowed)' AS title,
       'bar' AS type, 8 AS width;
SELECT 
    condition_name AS label,
    ROUND(CAST(total_medicare_payment AS REAL) / NULLIF(total_allowed_amt, 0) * 100, 1) AS value,
    color AS color
FROM mat_condition_national_summary
ORDER BY ROUND(CAST(total_medicare_payment AS REAL) / NULLIF(total_allowed_amt, 0) * 100, 1) DESC;

-- ── NEW: Source mix stacked breakdown ─────────────────────────────────────────
SELECT 'divider' AS component, 'Data Source Coverage' AS label;

SELECT 'chart' AS component,
       'Allowed Spend by Data Source — All Conditions' AS title,
       'bar' AS type, 6 AS width;
SELECT source_type AS label, SUM(total_allowed_amt) AS value
FROM mat_condition_source_breakdown
GROUP BY source_type
ORDER BY SUM(total_allowed_amt) DESC;

SELECT 'chart' AS component,
       'Beneficiaries by Data Source — All Conditions' AS title,
       'donut' AS type, 6 AS width;
SELECT source_type AS label, SUM(total_beneficiaries) AS value
FROM mat_condition_source_breakdown
GROUP BY source_type
ORDER BY SUM(total_beneficiaries) DESC;

-- ── Portfolio Summary Table ────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="condition-table-section"></div>' AS html;
SELECT 'divider' AS component, 'Full Portfolio Summary' AS label;

SELECT 'table' AS component, 
       'All Conditions Summary' AS title, 
       TRUE AS sort, TRUE AS search,
       'Condition' AS markdown;

SELECT
    '[' || condition_name || '](/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') || ')' AS "Condition",    
    specialty_domain                                             AS "Domain",
    'Tier ' || tier                                              AS "Tier",
    b2b_tier_primary                                             AS "Primary Specialty",
    printf('%,.0f', total_beneficiaries)                         AS "Beneficiaries",
    printf('%,.0f', total_services)                              AS "Services",
    '$' || printf('%,.0f', total_allowed_amt)                    AS "Allowed ($)",
    '$' || printf('%,.2f', allowed_per_patient)                  AS "$/Patient",
    ROUND(opportunity_score, 1)                                  AS "Opp. Score",
    data_sources                                                 AS "Sources"
FROM mat_condition_national_summary
ORDER BY tier, total_allowed_amt DESC;
```

---

## Opportunity Scoring

```sql mmi/opportunity-scoring.sql { route: { caption: "Opportunity Scores" } }

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
       'Opportunity Scoring' AS title,
       'Composite score: 40% beneficiary volume + 40% Medicare allowed spend + 20% strategic tier weighting.' AS description,
       'azure' AS color;

-- ── Top-3 Opportunity KPI Cards (replaces big_number for top picks) ───────────
SELECT 'divider' AS component, 'Top Opportunities' AS label;
SELECT 'card' AS component, 3 AS columns;

SELECT
    '#' || ROW_NUMBER() OVER (ORDER BY opportunity_score DESC) || ' — ' || condition_name AS title,
    ROUND(opportunity_score, 1) || ' / 100' AS description,
    color AS color, icon AS icon,
    'kpi-card' AS class,
    '/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') AS link
FROM mat_opportunity_score
ORDER BY opportunity_score DESC
LIMIT 3;

-- ── Charts ────────────────────────────────────────────────────────────────────
SELECT 'chart' AS component,
       'Opportunity Score by Condition' AS title,
       'bar' AS type, 8 AS width;
SELECT condition_name AS label, opportunity_score AS value, color AS color
FROM mat_opportunity_score
ORDER BY opportunity_score DESC;

-- NEW: Spend vs Volume scatter proxy (bar side-by-side with normalized values)
SELECT 'divider' AS component, 'Score Decomposition' AS label;

SELECT 'chart' AS component,
       'Beneficiary Volume Index (normalized 0–40)' AS title,
       'bar' AS type, 6 AS width;
SELECT 
    condition_name AS label,
    ROUND(CAST(total_benes AS REAL) / NULLIF((SELECT MAX(total_benes) FROM mat_opportunity_score), 0) * 40, 1) AS value,
    color AS color
FROM mat_opportunity_score
ORDER BY value DESC;

SELECT 'chart' AS component,
       'Spend Index (normalized 0–40)' AS title,
       'bar' AS type, 6 AS width;
SELECT 
    condition_name AS label,
    ROUND(CAST(total_allowed AS REAL) / NULLIF((SELECT MAX(total_allowed) FROM mat_opportunity_score), 0) * 40, 1) AS value,
    color AS color
FROM mat_opportunity_score
ORDER BY value DESC;

-- ── Ranked Table ──────────────────────────────────────────────────────────────
SELECT 'table' AS component, 'Ranked Opportunity Matrix' AS title, TRUE AS sort, TRUE AS search,
       'Condition' AS markdown;
SELECT
    ROW_NUMBER() OVER (ORDER BY opportunity_score DESC)  AS "Rank",    
    '[' || condition_name || '](/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') || ')' AS "Condition",  
    specialty_domain                                         AS "Domain",
    'Tier ' || tier                                          AS "Tier",
    b2b_tier_primary                                         AS "Primary Specialty",
    printf('%,.0f', total_benes)                             AS "Beneficiaries",
    '$' || printf('%,.0f', total_allowed)                    AS "Allowed ($)",
    ROUND(opportunity_score, 1)                              AS "Score /100"
FROM mat_opportunity_score;
```

---

## Disease Conditions Directory

```sql mmi/conditions.sql { route: { caption: "Disease Conditions" } }

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
       'Disease Condition Registry' AS title,
       'All conditions active in the pipeline. Click any condition to explore its market data.' AS description,
       'indigo' AS color;

-- ── Summary KPI Cards for Registry ───────────────────────────────────────────
SELECT 'card' AS component, 4 AS columns;

SELECT 'Active Conditions' AS title,
       COUNT(*) || '' AS description,
       'teal' AS color, 'activity' AS icon,
       'kpi-card' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1;

SELECT 'Tier 1 — Flagship' AS title,
       COUNT(*) || '' AS description,
       'orange' AS color, 'star' AS icon,
       'kpi-card' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1 AND tier = 1;

SELECT 'Tier 2 — Core' AS title,
       COUNT(*) || '' AS description,
       'blue' AS color, 'layers' AS icon,
       'kpi-card' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1 AND tier = 2;

SELECT 'Tier 3 — Baseline' AS title,
       COUNT(*) || '' AS description,
       'grape' AS color, 'list' AS icon,
       'kpi-card' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1 AND tier = 3;

-- ── Condition Cards ───────────────────────────────────────────────────────────
SELECT 'card' AS component, 3 AS columns;
SELECT
    r.condition_name                                         AS title,
    r.body_system || ' | ' || r.specialty_domain            AS subtitle,
    'ICD-10: ' || COALESCE(r.icd10_prefix, '—')
        || ' | Tier: ' || r.tier
        || ' | Target: ' || COALESCE(r.b2b_tier_primary, '—') AS description,
    '/mmi/condition-hub.sql?condition=' || REPLACE(r.condition_name, ' ', '%20') AS link,
    r.icon                                                   AS icon,
    r.color                                                  AS color
FROM dim_condition_registry r
WHERE r.is_active = 1
ORDER BY r.tier, r.condition_name;

-- ── Registry Table ────────────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="registry-table"></div>' AS html;
SELECT 'divider' AS component, 'Registry Table' AS label;
SELECT 'table' AS component, 'dim_condition_registry' AS title, TRUE AS sort, TRUE AS search;
SELECT
    condition_name                                          AS "Condition",
    body_system                                             AS "Body System",
    tier                                                    AS "Tier",
    icd10_prefix                                            AS "ICD-10 Prefix",
    hcpcs_range_start || '–' || hcpcs_range_end            AS "HCPCS Range",
    b2b_tier_primary                                        AS "Primary Specialty",
    CASE use_bygeo   WHEN 1 THEN '✓' ELSE '—' END          AS "GEO",
    CASE use_dmepos  WHEN 1 THEN '✓' ELSE '—' END          AS "DME",
    CASE use_hospital WHEN 1 THEN '✓' ELSE '—' END         AS "Hospital",
    CASE is_active   WHEN 1 THEN 'Active' ELSE 'Inactive' END AS "Status"
FROM dim_condition_registry
ORDER BY tier, condition_name;
```

---

## Procedure Drilldown

```sql mmi/procedure-drilldown.sql { route: { caption: "Procedure Drilldown" } }

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SET limit = 20;
SET current_page = COALESCE(CAST(:page AS INT), 1);
SET offset = ($current_page - 1) * $limit;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
       'Procedure Code Drilldown' AS title,
       'HCPCS-level analytics across all conditions.' AS description,
       'orange' AS color;

-- ── Procedure KPI Cards (replaces big_number) ─────────────────────────────────
SELECT 'card' AS component, 4 AS columns;

SELECT 'Total Procedures' AS title,
       printf('%,.0f', (SELECT COUNT(DISTINCT hcpcs_code) FROM mat_condition_hcpcs_detail)) AS description,
       'orange' AS color, 'clipboard-list' AS icon,
       'kpi-card' AS class,
       '#procedure-table' AS link;

SELECT 'Total Allowed ($)' AS title,
       '$' || printf('%,.0f', (SELECT SUM(total_allowed_amt) FROM mat_condition_hcpcs_detail)) AS description,
       'indigo' AS color, 'currency-dollar' AS icon,
       'kpi-card' AS class,
       '#procedure-table' AS link;

SELECT 'Total Services' AS title,
       printf('%,.0f', (SELECT SUM(total_services) FROM mat_condition_hcpcs_detail)) AS description,
       'teal' AS color, 'activity' AS icon,
       'kpi-card' AS class,
       '#procedure-table' AS link;

SELECT 'Total Beneficiaries' AS title,
       printf('%,.0f', (SELECT SUM(total_beneficiaries) FROM mat_condition_hcpcs_detail)) AS description,
       'azure' AS color, 'users' AS icon,
       'kpi-card' AS class,
       '#procedure-table' AS link;

-- ── NEW: Top Categories Chart ─────────────────────────────────────────────────
SELECT 'chart' AS component,
       'Top 10 Procedures by Allowed Spend' AS title,
       'bar' AS type, 12 AS width;
SELECT 
    hcpcs_code || ' — ' || COALESCE(SUBSTR(procedure_description, 1, 35), '') AS label,
    SUM(total_allowed_amt) AS value
FROM mat_condition_hcpcs_detail
WHERE (:condition IS NULL OR TRIM(LOWER(condition_name)) = TRIM(LOWER(:condition)))
GROUP BY hcpcs_code, procedure_description
ORDER BY SUM(total_allowed_amt) DESC
LIMIT 10;

-- ── Procedure Table ───────────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="procedure-table"></div>' AS html;

SELECT 'table' AS component, 
       'Top Procedures by Allowed Spend' AS title, 
       TRUE AS sort, TRUE AS search,
       'HCPCS' AS markdown,      
       'Condition' AS markdown;

SELECT
    hcpcs_code AS "HCPCS",
    COALESCE(procedure_description, 'DRG ' || hcpcs_code) AS "Description",
    procedure_category AS "Category",    
    '[' || condition_name || '](/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') || ')' AS "Condition",    
    source_type AS "Source",
    printf('%,.0f', total_services) AS "Services",
    printf('%,.0f', total_beneficiaries) AS "Beneficiaries",
    '$' || printf('%,.0f', total_allowed_amt) AS "Total Allowed",
    '$' || printf('%,.2f', avg_allowed_per_service) AS "$/Service"
FROM mat_condition_hcpcs_detail
WHERE (:condition IS NULL OR TRIM(LOWER(condition_name)) = TRIM(LOWER(:condition)))
  AND (:hcpcs IS NULL OR hcpcs_code = :hcpcs)
ORDER BY total_allowed_amt DESC
LIMIT $limit
OFFSET $offset;

SET total_rows = (SELECT COUNT(*) FROM mat_condition_hcpcs_detail 
    WHERE (:condition IS NULL OR TRIM(LOWER(condition_name)) = TRIM(LOWER(:condition))));
SET total_pages = ($total_rows + $limit - 1) / $limit;

SELECT 'pagination' AS component,
    ($current_page <= 1) AS previous_disabled,
    ($current_page >= $total_pages) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page', $current_page - 1, 'condition', :condition)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page', $current_page + 1, 'condition', :condition)) AS next_link;

WITH RECURSIVE page_numbers AS (
    SELECT MAX(1, $current_page - 5) AS n
    UNION ALL
    SELECT n + 1 FROM page_numbers WHERE n < MIN($total_pages, $current_page + 5)
)
SELECT n AS contents,
       sqlpage.link(sqlpage.path(), json_object('page', n, 'condition', :condition)) AS link,
       (n = $current_page) AS active
FROM page_numbers;
```

---

## Geography

```sql mmi/geography.sql { route: { caption: "Geography" } }

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
       'Geographic Intelligence' AS title,
       'State-level market sizing, cost tiers, and GPCI factors across all conditions.' AS description,
       'indigo' AS color;

-- ── Geography KPI Cards ───────────────────────────────────────────────────────
SELECT 'card' AS component, 4 AS columns;

SELECT 'States with Data' AS title,
       COUNT(DISTINCT state_abbr) || '' AS description,
       'indigo' AS color, 'map-pin' AS icon,
       'kpi-card' AS class,
       '#state-table' AS link
FROM mat_condition_state_breakdown;

SELECT 'Top State Spend' AS title,
       '$' || printf('%,.0f', MAX(state_total)) AS description,
       'teal' AS color, 'trending-up' AS icon,
       'kpi-card' AS class,
       '#state-table' AS link
FROM (SELECT state_abbr, SUM(total_allowed_amt) AS state_total FROM mat_condition_state_breakdown GROUP BY state_abbr);

SELECT 'Avg Allowed/Patient' AS title,
       '$' || printf('%,.0f', AVG(allowed_per_patient)) AS description,
       'azure' AS color, 'calculator' AS icon,
       'kpi-card' AS class,
       '#state-table' AS link
FROM mat_condition_state_breakdown WHERE allowed_per_patient > 0;

SELECT 'Disease Conditions Covered' AS title,
       COUNT(DISTINCT condition_name) || '' AS description,
       'orange' AS color, 'activity' AS icon,
       'kpi-card' AS class,
       '#state-table' AS link
FROM mat_condition_state_breakdown;

-- ── Charts ────────────────────────────────────────────────────────────────────
SELECT 'chart' AS component,
       'Top 15 States by Total Allowed Spend (All Conditions)' AS title,
       'bar' AS type, 12 AS width;
SELECT state_abbr AS label, SUM(total_allowed_amt) AS value
FROM mat_condition_state_breakdown
GROUP BY state_abbr
ORDER BY SUM(total_allowed_amt) DESC
LIMIT 15;

-- NEW: Top states by beneficiary count
SELECT 'chart' AS component,
       'Top 15 States by Beneficiary Count' AS title,
       'bar' AS type, 12 AS width;
SELECT state_abbr AS label, SUM(total_beneficiaries) AS value
FROM mat_condition_state_breakdown
GROUP BY state_abbr
ORDER BY SUM(total_beneficiaries) DESC
LIMIT 15;

-- ── State Market Summary Table ────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="state-table"></div>' AS html;

SET max_per_page_obj = 10;
SET count_obj = (SELECT COUNT(DISTINCT state_abbr || '|' || COALESCE(locality_name,'')) FROM mat_condition_state_breakdown);
SET pages_obj = (CAST($count_obj AS INT) / $max_per_page_obj) + (CASE WHEN ($count_obj % $max_per_page_obj) = 0 THEN 0 ELSE 1 END);
SET current_page_obj = COALESCE(CAST($page_obj AS INT), 1);

SELECT 'table' AS component, 'State Market Summary' AS title, TRUE AS sort, TRUE AS search;
SELECT
    state_abbr                                              AS "State",
    COALESCE(NULLIF(locality_name, ''), 'Statewide')        AS "Locality",
    MAX(cost_tier)                                          AS "Cost Tier",
    MAX(pw_gpci)                                            AS "GPCI",
    COUNT(DISTINCT condition_name)                          AS "Disease Conditions",
    printf('%,.0f', SUM(total_beneficiaries))               AS "Beneficiaries",
    '$' || printf('%,.0f', SUM(total_allowed_amt))          AS "Allowed ($)"
FROM mat_condition_state_breakdown
GROUP BY state_abbr, locality_name
ORDER BY SUM(total_allowed_amt) DESC
LIMIT $max_per_page_obj
OFFSET ($current_page_obj - 1) * $max_per_page_obj;

SELECT 'pagination' AS component,
    ($current_page_obj = 1) AS previous_disabled,
    ($current_page_obj = $pages_obj) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page_obj', $current_page_obj - 1, 'page_idx', $page_idx)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page_obj', $current_page_obj + 1, 'page_idx', $page_idx)) AS next_link;

WITH RECURSIVE page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM page_numbers WHERE n < $pages_obj
)
SELECT n AS contents, 
       sqlpage.link(sqlpage.path(), json_object('page_obj', n, 'page_idx', $page_idx)) AS link,
       (n = $current_page_obj) AS active FROM page_numbers;
```

---

## Data Dictionary

```sql mmi/data-dictionary.sql { route: { caption: "Data Dictionary" } }

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon, 'fluid' AS layout, true AS fixed_top_menu,
       CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
       '/custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Disease Conditions"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Geography"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Data Dictionary & Pipeline Reference' AS title,
    'Schema reference for the Medigy Disease State Database (MDSD).' AS description,
    'gray' AS color;

-- ── Schema Object Count Cards (replaces big_number) ───────────────────────────
-- Each card links to the relevant section below via same-page anchors
SELECT 'card' AS component, 4 AS columns;

SELECT 'Total Schema Objects' AS title, 
       COUNT(*) || '' AS description,
       'blue' AS color, 'database' AS icon,
       'dict-stat-card' AS class,
       '#objects-table' AS link
FROM data_tables_derived;

SELECT 'Materialized Tables' AS title,
       COUNT(*) || '' AS description,
       'teal' AS color, 'table' AS icon,
       'dict-stat-card' AS class,
       '#objects-table' AS link
FROM data_tables_derived WHERE category = 'Materialized Table';

SELECT 'Core Fact Tables' AS title,
       COUNT(*) || '' AS description,
       'indigo' AS color, 'layers' AS icon,
       'dict-stat-card' AS class,
       '#objects-table' AS link
FROM data_tables_derived WHERE category = 'Core Fact';

SELECT 'Performance Indexes' AS title,
       COUNT(*) || '' AS description,
       'orange' AS color, 'bolt' AS icon,
       'dict-stat-card' AS class,
       '#indexes-table' AS link
FROM data_dictionary_indexes;

-- ── Data Sources ──────────────────────────────────────────────────────────────
SELECT 'text' AS component, 'External Data Sources' AS contents_md;
SELECT 'list' AS component;
SELECT 
    title, description, link,
    'external-link' AS icon, 'blue' AS color
FROM data_provenance 
WHERE object_type = 'external_source';

-- NEW: Data freshness cards
SELECT 'divider' AS component, 'Data Ingestion Timeline' AS contents;
SELECT 'card' AS component, 3 AS columns;
SELECT 
    title AS title,
    DATE(ingested_at) AS description,
    'blue' AS color,
    'calendar' AS icon
FROM data_provenance
WHERE object_type = 'external_source'
ORDER BY ingested_at DESC;

-- ── Master & Reference Tables ─────────────────────────────────────────────────
SELECT 'text' AS component, 'Master & Reference Tables' AS contents_md;
SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;
SELECT name AS "Table Name"
FROM sqlite_schema s
WHERE (name LIKE 'dim_%' OR name LIKE 'uniform_resource_ref_%')
ORDER BY name;

-- ── Derived Objects Inventory ─────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="objects-table"></div>' AS html;
SELECT 'title' AS component, 'Schema Data Dictionary' AS contents;

SET max_per_page_obj = 10;
SET count_obj = (SELECT COUNT(*) FROM data_tables_derived);
SET pages_obj = (CAST($count_obj AS INT) / $max_per_page_obj) + (CASE WHEN ($count_obj % $max_per_page_obj) = 0 THEN 0 ELSE 1 END);
SET current_page_obj = COALESCE(CAST($page_obj AS INT), 1);

SELECT 'table' AS component, 
       TRUE AS sort, TRUE AS search, TRUE AS markdown,
       'Derived Objects Inventory' AS title;

SELECT 
    object_name, object_type, category
FROM data_tables_derived
ORDER BY category, object_name
LIMIT $max_per_page_obj
OFFSET ($current_page_obj - 1) * $max_per_page_obj;

SELECT 'pagination' AS component,
    ($current_page_obj = 1) AS previous_disabled,
    ($current_page_obj = $pages_obj) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page_obj', $current_page_obj - 1, 'page_idx', $page_idx)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page_obj', $current_page_obj + 1, 'page_idx', $page_idx)) AS next_link;

WITH RECURSIVE page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM page_numbers WHERE n < $pages_obj
)
SELECT n AS contents, 
       sqlpage.link(sqlpage.path(), json_object('page_obj', n, 'page_idx', $page_idx)) AS link,
       (n = $current_page_obj) AS active FROM page_numbers;

-- ── Performance Indexes ───────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="indexes-table"></div>' AS html;
SELECT 'text' AS component, 'Query Performance Indexes' AS contents_md;

SET max_per_page_idx = 10;
SET count_idx = (SELECT COUNT(*) FROM data_dictionary_indexes);
SET pages_idx = (CAST($count_idx AS INT) / $max_per_page_idx) + (CASE WHEN ($count_idx % $max_per_page_idx) = 0 THEN 0 ELSE 1 END);
SET current_page_idx = COALESCE(CAST($page_idx AS INT), 1);

SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;
SELECT 
    index_name AS "Index Name",
    table_name AS "Target Table",
    description AS "Description"
FROM data_dictionary_indexes
LIMIT $max_per_page_idx
OFFSET ($current_page_idx - 1) * $max_per_page_idx;

SELECT 'pagination' AS component,
    ($current_page_idx = 1) AS previous_disabled,
    ($current_page_idx = $pages_idx) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page_idx', $current_page_idx - 1, 'page_obj', $page_obj)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page_idx', $current_page_idx + 1, 'page_obj', $page_obj)) AS next_link;

WITH RECURSIVE idx_page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM idx_page_numbers WHERE n < $pages_idx
)
SELECT n AS contents, 
       sqlpage.link(sqlpage.path(), json_object('page_idx', n, 'page_obj', $page_obj)) AS link,
       (n = $current_page_idx) AS active FROM idx_page_numbers;

-- ── Materialized Tables ───────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="materialized-table"></div>' AS html;
SELECT 'text' AS component, 'Materialized Tables' AS contents_md;

SET max_per_page_mat = 10;
SET count_mat = (SELECT COUNT(*) FROM data_tables_derived WHERE category = 'Materialized Table' );
SET pages_mat = (CAST($count_mat AS INT) / $max_per_page_mat) + (CASE WHEN ($count_mat % $max_per_page_mat) = 0 THEN 0 ELSE 1 END);
SET current_page_mat = COALESCE(CAST($page_mat AS INT), 1);

SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;
SELECT 
    object_name, object_type, category
FROM data_tables_derived
 WHERE category = 'Materialized Table' 
ORDER BY category, object_name 
LIMIT $max_per_page_mat
OFFSET ($current_page_mat - 1) * $max_per_page_mat;

SELECT 'pagination' AS component,
    ($current_page_mat = 1) AS previous_disabled,
    ($current_page_mat = $pages_mat) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page_mat', $current_page_mat - 1, 'page_idx', $page_idx)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page_mat', $current_page_mat + 1, 'page_idx', $page_idx)) AS next_link;

WITH RECURSIVE mat_page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM mat_page_numbers WHERE n < $pages_mat
)
SELECT n AS contents, 
       sqlpage.link(sqlpage.path(), json_object('page_mat', n, 'page_idx', $page_idx)) AS link,
       (n = $current_page_mat) AS active FROM mat_page_numbers;
```
