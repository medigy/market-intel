---
sqlpage-conf:
  database_url: "sqlite://resource-surveillance.sqlite.db?mode=rwc"
  web_root: "./dev-src.auto"
  allow_exec: true
  port: 9227
---
# Medigy Market Intelligence — Unified SQLPage Application 

This application is built on the **unified extensible pipeline** (`medigy-unified-v2.sql`).

**Architecture principle:** Adding a new disease condition requires inserting
one row into `dim_condition_registry`. The landing page, drilldown pages,
and all analytics update automatically — no SQL or page logic changes required.

---

**v3 changes:**

- All `big_number` components replaced with navigable `card` components
- Slow analytics views materialized as indexed tables (`mat_*`)
- External CSS (`custom-dashboard.css`) applied across all pages
- New visualizations: opportunity scatter, tier distribution donut,
  source-mix bar, top-state treemap equivalent, data freshness timeline
- Same-page anchor navigation via HTML anchors + card links

---

## Spry Axiom Configuration

```code DEFAULTS
sql * --interpolate --injectable
```

## Setup

```bash prepare-db-deploy-server --descr "Ingest raw files, build unified analytics, launch SQLPage UI."
#!/bin/bash
set -euo pipefail

rm -f resource-surveillance.sqlite.*

surveilr ingest files -r medicare-ds/ 
surveilr orchestrate transform-csv
surveilr shell sql/medigy-unified-v2.sql
surveilr shell sql/medigy-ddl.sql
spry sp spc --package --conf sqlpage/sqlpage.json -m mmi-dashboard.md | sqlite3 resource-surveillance.sqlite.db
echo "Medigy Market Intelligence (v3) is ready."
```

---

## Layout

```sql PARTIAL global-layout.sql --inject mmi/*.sql

-- BEGIN: PARTIAL global-layout.sql
-- Using root-relative paths (starting with '/') ensures that these files 
-- are resolved correctly by SQLPage regardless of the deployment base URL.
SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
       '/' AS link,
       '../footer-links.js' AS javascript,
       '../custom-dashboard.css' AS css,
       '© 2026 Medigy Market Intelligence' AS footer,
       'upgrade-insecure-requests' AS header_content_security_policy,
       '{"link":"/mmi/home-overview.sql","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Insights"}' AS menu_item,
       '{"link":"/mmi/conditions.sql","title":"Clinical Portfolio"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Market Prioritization"}' AS menu_item,
       '{"link":"/mmi/geography.sql","title":"Regional Intelligence"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Tactical Analytics"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Provenance"}' AS menu_item;


SET resource_json = sqlpage.read_file_as_text('spry.d/auto/resource/${path}.auto.json');
SET page_title  = json_extract($resource_json, '$.route.caption');
SET page_description  = json_extract($resource_json, '$.route.description');
SET page_path = json_extract($resource_json, '$.route.path');


-- END: PARTIAL global-layout.sql

```

```contribute sqlpage_files --base .
./footer-links.js .
./custom-dashboard.css .
```

---

## Registration Page

```sql index.sql { route: { caption: "Registration" } }
-- @route.description "User registration gate before entering the dashboard"

-- Early server-side redirect: if the registration profile cookie is already present,
-- skip the form immediately — this prevents the shell/hero/form from being rendered
-- and sent to the browser, eliminating the flicker that occurred when the JS redirect
-- in footer-links.js fired after the page was already painted.
-- Relative redirect: browser resolves against the current URL, so it works at any deployment base path.
SELECT 'redirect' AS component,
       'mmi/home-overview.sql' AS link
WHERE COALESCE(sqlpage.cookie('medigy_mmi_registration_profile_v2'), '') != '';

SELECT 'cookie' AS component,
       'isVerified' AS name,
       'false' AS value,
       '/' AS path,
       'lax' AS same_site,
       TRUE AS secure
WHERE COALESCE(sqlpage.cookie('isVerified'), '') = '';

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'narrow' AS layout,
       true AS fixed_top_menu,
       './' AS link,
    './footer-links.js' AS javascript,
       'upgrade-insecure-requests' AS header_content_security_policy,
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
    'last_name' AS name,
    'Last Name' AS label,
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
SET submitted_last_name = COALESCE(NULLIF($last_name, ''), '');
SET submitted_email_address = COALESCE(NULLIF($email_address, ''), '');
SET submitted_phone_number = TRIM(COALESCE(NULLIF(NULLIF(TRIM($phone_number), ''), '+1'), ''));
SET submitted_phone_number_sanitized = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($submitted_phone_number, '+', ''), ' ', ''), '-', ''), '(', ''), ')', ''), '.', '');
SET submitted_phone_digits_stripped = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($submitted_phone_number_sanitized,'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9','');
SET submitted_organization = COALESCE(NULLIF($organization, ''), '');
SET submitted_purpose_of_visit = COALESCE(NULLIF($purpose_of_visit, ''), '');
SET submitted_consent_acknowledged = LOWER(TRIM(COALESCE(NULLIF($consent_acknowledged, ''), '')));
SET submitted_full_name = TRIM($submitted_first_name || ' ' || $submitted_last_name);
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
    'cat > /tmp/smtp_msg_$$.txt << ''EOF'' ' || CHAR(10) ||
    'From: ' || $smtp_from || CHAR(10) ||
    'To: ' || $recipient_email || CHAR(10) ||
    'Subject: New User Access Notification – Medigy Market Intelligence (MMI) Application Entry' || CHAR(10) ||
    'MIME-Version: 1.0' || CHAR(10) ||
    'Content-Type: text/html; charset=UTF-8' || CHAR(10) || CHAR(10) ||
    '<html><body>' || CHAR(10) ||
    '<p>Hi Team,</p>' || CHAR(10) ||
    '<p>A new user has accessed Medigy Market Intelligence (MMI) Application and submitted their details.</p>' || CHAR(10) ||
    '<p><strong>User Information:</strong></p>' || CHAR(10) ||
    '<ul>' || CHAR(10) ||
    '<li><strong>Full Name:</strong> ' || REPLACE($submitted_full_name, '''', '&apos;') || '</li>' || CHAR(10) ||
    '<li><strong>Email Address:</strong> ' || REPLACE($submitted_email_address, '''', '&apos;') || '</li>' || CHAR(10) ||
    '<li><strong>Phone Number:</strong> ' || REPLACE($submitted_phone_number, '''', '&apos;') || '</li>' || CHAR(10) ||
    '<li><strong>Organization / Company:</strong> ' || REPLACE($submitted_organization, '''', '&apos;') || '</li>' || CHAR(10) ||
    '<li><strong>Purpose of Visit:</strong> ' || REPLACE($submitted_purpose_of_visit, '''', '&apos;') || '</li>' || CHAR(10) ||
    '</ul>' || CHAR(10) ||
    '<p><strong>Access Details:</strong></p>' || CHAR(10) ||
    '<ul>' || CHAR(10) ||
    '<li><strong>Date &amp; Time:</strong> ' || REPLACE($submitted_access_timestamp, '''', '&apos;') || '</li>' || CHAR(10) ||
    '<li><strong>IP Address:</strong> ' || REPLACE($submitted_ip_address, '''', '&apos;') || '</li>' || CHAR(10) ||
    '<li><strong>Device / Browser:</strong> ' || REPLACE($submitted_user_agent, '''', '&apos;') || '</li>' || CHAR(10) ||
    '</ul>' || CHAR(10) ||
    '<p><strong>Notes:</strong>
This notification is generated automatically when a user enters their details on the MMI Application entry screen. The information can be used for follow-up communication, support, or engagement purposes.</p>' || CHAR(10) ||
    '<p>Please reach out to the user if required.</p>' || CHAR(10) ||
    '<p>Regards,
System Notification</p>' || CHAR(10) ||
    '</body></html>' || CHAR(10) ||
    'EOF' || CHAR(10) ||
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

-- Relative redirect: browser resolves against the current URL, so it works at any deployment base path.
SELECT 'redirect' AS component,
       'mmi/home-overview.sql' AS link
WHERE $email_is_valid = 1 AND $phone_is_valid = 1 AND $consent_is_valid = 1;
```

---

## Home Page

```sql registration.sql { route: { caption: "Registration Alias" } }
-- @route.description "Alias route for user registration gate"

-- Early server-side redirect: mirrors the same guard in index.sql.
-- Relative redirect: browser resolves against the current URL, so it works at any deployment base path.
SELECT 'redirect' AS component,
       'mmi/home-overview.sql' AS link
WHERE COALESCE(sqlpage.cookie('medigy_mmi_registration_profile_v2'), '') != '';

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
    '../footer-links.js' AS javascript,
    '../custom-dashboard.css' AS css,
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
    'last_name' AS name,
    'Last Name' AS label,
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

## Strategic Overview Page

```sql mmi/home-overview.sql { route: { caption: "Strategic Overview" } }
-- @route.description "MMI landing page — portfolio KPIs, condition cards, navigation"

-- Hero
SELECT 'hero' AS component,
       'Medigy Market Intelligence' AS title,
       'Empowering healthcare leadership with high-fidelity market analytics. Powered by the latest audited CMS datasets (2023) and multi-stream clinical registries to identify untapped growth with mathematical precision.' AS description,
       'https://kmgus.com/wp-content/uploads/2022/06/Data-Analytics-in-Healthcare-Edited.jpg' AS image,
       'primary' AS color;

-- We frame this as "Operational Truth" rather than just a single year.
SELECT 'alert' AS component,
       'Source of Truth: 2023 CMS Annual Release' AS title,
       'All intelligence is anchored in audited 2023 CMS Public Use Files. Our multi-source engine synthesizes varied data vintages to provide a unified, evidence-based view of the healthcare landscape.' AS description,
       'info' AS color, 'database-check' AS icon;

SELECT 'divider' AS component, '2023 Market Performance Indicators' AS label;
SELECT 'text' AS component, 'Real-World Scale' AS title, 
       'These metrics represent the verified Medicare scale across patients, providers, and financial impact for the 2023 fiscal year.' AS contents;

SELECT 'card' AS component, 4 AS columns;

-- Row 1: The High-Level Totals
SELECT 'Active Disease Markets' AS title, printf('%,.0f', total_conditions) AS description, 
       '2023 Clinical Registry' AS footer, 'teal' AS color, 'activity' AS icon, 'kpi-card' AS class, '/mmi/conditions.sql' AS link FROM mat_executive_kpis;

SELECT 'Patients Impacted' AS title, printf('%,.1f', total_beneficiaries / 1000000.0) || 'M' AS description, 
       'Medicare Part B Scope' AS footer, 'azure' AS color, 'users' AS icon, 'kpi-card kpi-azure' AS class, '/mmi/executive-dashboard.sql' AS link FROM mat_executive_kpis;

SELECT 'Total Market Spend ($)' AS title, '$' || printf('%,.1f', total_allowed_amt / 1000000000.0) || 'B' AS description, 
       'Annual Allowed Amount' AS footer, 'indigo' AS color, 'currency-dollar' AS icon, 'kpi-card kpi-indigo' AS class, '/mmi/executive-dashboard.sql' AS link FROM mat_executive_kpis;

SELECT 'Data Sources Integrated' AS title, (SELECT COUNT(*) FROM data_provenance WHERE object_type = 'external_source') || ' Sources' AS description, 
       'Multi-Stream Provenance' AS footer, 'orange' AS color, 'database' AS icon, 'kpi-card kpi-orange' AS class, '/mmi/data-dictionary.sql' AS link;

-- Row 2: Tactical Breadth
SELECT 'Geographic Reach' AS title, printf('%,.0f', total_states) || ' States' AS description, 
       'Regional Intelligence' AS footer, 'blue' AS color, 'map-pin' AS icon, 'kpi-card kpi-blue' AS class, '/mmi/geography.sql' AS link FROM mat_executive_kpis;

SELECT 'Clinical Procedures' AS title, printf('%,.0f', total_procedures) AS description, 
       'CPT/HCPCS Tracking' AS footer, 'grape' AS color, 'clipboard-list' AS icon, 'kpi-card kpi-grape' AS class, '/mmi/procedure-drilldown.sql' AS link FROM mat_executive_kpis;

SELECT 'Actual Medicare Payout' AS title, '$' || printf('%,.1f', total_medicare_payment / 1000000000.0) || 'B' AS description, 
       'Net Federal Expenditure' AS footer, 'teal' AS color, 'receipt' AS icon, 'kpi-card' AS class, '/mmi/executive-dashboard.sql#payment-section' AS link FROM mat_executive_kpis;

SELECT 'Ranked Growth Targets' AS title, (SELECT COUNT(*) FROM mat_opportunity_score) || ' Targets' AS description, 
       'ROI-Driven Ranking' AS footer, 'orange' AS color, 'trophy' AS icon, 'kpi-card kpi-orange' AS class, '/mmi/opportunity-scoring.sql' AS link;

---
--- 5. VISUAL INTELLIGENCE (Business Narration)
---
SELECT 'divider' AS component, 'Market Opportunity Analysis' AS contents;
SELECT 'text' AS component, 'Identifying the Growth Frontier' AS title, 
       'We translate $B in claims into actionable insights. The charts below deconstruct the market by opportunity score, strategic tiering, and care-setting flow.' AS contents;

-- Treemap for Opportunity: Professional, handles varying scales well
SELECT 'chart' AS component, 'Top 10 Disease Markets by Opportunity' AS title, 'treemap' AS type, 8 AS width;
SELECT 
    specialty_domain AS series,
    condition_name AS label, 
    opportunity_score AS value
FROM mat_condition_national_summary 
ORDER BY opportunity_score DESC LIMIT 10;


-- Donut for Portfolio: Catchy, standard business share visual
SELECT 'chart' AS component, 'Portfolio Mix by Strategic Tier' AS title, 'donut' AS type, 4 AS width;
SELECT 'Tier ' || tier || ' — ' || CASE tier WHEN 1 THEN 'Flagship' WHEN 2 THEN 'Core' ELSE 'Baseline' END AS label,
    SUM(total_allowed_amt) AS value FROM mat_condition_national_summary GROUP BY tier;

-- Care Setting Spend: Treemap to handle the massive GEO vs DME scale difference
SELECT 
    'chart' AS component, 
    'Market Spend Magnitude by Care Setting ($ Billions)' AS title, 
    'bar' AS type, 
    TRUE AS horizontal,
    12 AS width;

SELECT 
    CASE 
        WHEN source_type LIKE '%DME%' THEN 'Medical Equipment'
        WHEN source_type LIKE '%GEO%' THEN 'Geographic Services'
        WHEN source_type LIKE '%HOSP%' THEN 'Hospital/Facility'
        ELSE 'Specialized Diagnostics'
    END AS series,
    source_type AS label, 
    -- Scaling raw value to Billions for X-axis readability
    SUM(total_allowed_amt) / 1000000000.0 AS value
FROM mat_condition_source_breakdown 
GROUP BY source_type, series 
ORDER BY value DESC;
---
--- 6. EXPLORE MARKETS (Condition Cards)
---
SELECT 'html' AS component, '<div id="disease-conditions-section"></div>' AS html;
SELECT 'divider' AS component, 'Strategic Market Registry' AS contents;

SELECT 'card' AS component, 3 AS columns;
SELECT
    s.condition_name AS title,
    s.specialty_domain || ' | Tier ' || s.tier AS subtitle,
    '$' || printf('%,.0f', s.total_allowed_amt) || ' Allowed Spend' AS description,
    '/mmi/condition-hub.sql?condition=' || REPLACE(s.condition_name, ' ', '%20') AS link,
    s.icon AS icon, s.color AS color
FROM mat_condition_national_summary s
ORDER BY s.tier, s.opportunity_score DESC;

---
--- 7. RESTORED ANALYTICS NAVIGATION (Action-Oriented)
---
SELECT 'divider' AS component, 'Strategic Execution & Growth Workflows' AS contents;
SELECT 'card' AS component, 3 AS columns;

SELECT 'Executive Insights' AS title, 'Analyze total market size and patient volume trends.' AS description, '/mmi/executive-dashboard.sql' AS link, 'layout-dashboard' AS icon, 'teal' AS color;
SELECT 'Market Priortization' AS title, 'Identify the most promising clinical markets using 2023 evidence.' AS description, '/mmi/opportunity-scoring.sql' AS link, 'trophy' AS icon, 'azure' AS color;
SELECT 'Geographic Intelligence' AS title, 'Pinpoint high-growth regions and regional underserved markets.' AS description, '/mmi/geography.sql' AS link, 'map-pin' AS icon, 'indigo' AS color;
SELECT 'Tactical Analytics' AS title, 'Deep-dive into care delivery patterns at the procedure level.' AS description, '/mmi/procedure-drilldown.sql' AS link, 'clipboard-list' AS icon, 'orange' AS color;
SELECT 'Clinical Portfolio' AS title, 'Explore and compare the full catalog of healthcare markets.' AS description, '/mmi/conditions.sql' AS link, 'virus' AS icon, 'grape' AS color;
SELECT 'Data Provenance' AS title, 'Audit the 2023 CMS datasets, table schemas, and pipeline logic.' AS description, '/mmi/data-dictionary.sql' AS link, 'book' AS icon, 'blue' AS color;

```

---

## Condition Hub — Universal Disease Drilldown Page

```sql mmi/condition-hub.sql { route: { caption: "Condition Hub" } }

SELECT 'button' AS component, 'start' AS justify;
-- SELECT 'Home' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;
SELECT 'Home' AS title, '/mmi/home-overview.sql' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;


SELECT 'hero' AS component,
       s.condition_name AS title,
       'Strategic Clinical Domain: ' || s.specialty_domain || ' | Primary Focus: ' || s.b2b_tier_primary
           || ' | Priority Tier ' || s.tier AS description,
        'https://sa1s3optim.patientpop.com/640x/filters:format(webp)/assets/production/practices/178ad3a03c4e94f9ed1363ffcbd41385a2a4a616/images/2809637.jpg' as image,
       s.color AS color
FROM mat_condition_national_summary s
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
LIMIT 1;

-- Operational Truth Alert (Manager's Branding)
SELECT 'alert' AS component,
       'Market Integrity: 2023 CMS Evidence' AS title,
       'The analytics below represent the verified 2023 market activity for ' || $condition || '. Use these insights to validate patient density and financial intensity across care settings.' AS description,
       'info' AS color, 'shield-check' AS icon;

-- Executive Narrative
SELECT 'text' AS component, 'Market Strategy Overview' AS title;
SELECT 'This intelligence briefing provides a high-fidelity view of the ' || $condition || ' marketplace. We synthesize patient volume, financial magnitude, and geographic concentration to de-risk your clinical growth strategy.' AS contents;



-- ── National KPI Strip — Navigable Cards (replaces big_number) ────────────────
-- Anchor links allow same-page navigation to tables below
SELECT 'html' AS component, '<div id="national-kpi-section"></div>' AS html;
SELECT 'divider' AS component, 'National Market Magnitude' AS contents;

SELECT 'text' AS component,
'The following metrics summarize overall market scale and economic impact. High spend per patient combined with high service utilization typically indicates chronic management or device-driven care pathways.' 
AS contents_md;

SELECT 'card' AS component, 4 AS columns;
SELECT 'Addressable Market Size' AS title,
       printf('%,.0f', total_beneficiaries) AS description,
       'Total Patient Population' AS footer,
       color AS color, 'users' AS icon,
       'kpi-hub-card' AS class,
       '#geo-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Service Intensity' AS title,
       printf('%,.0f', total_services) AS description,
       'Total Procedures Delivered' AS footer,
       color AS color, 'activity' AS icon,
       'kpi-hub-card' AS class,
       '#procedures-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Annual Spending Magnitude' AS title,
       '$' || printf('%,.0f', total_allowed_amt) AS description,
       'Medicare Allowed Amount' AS footer,
       color AS color, 'currency-dollar' AS icon,
       'kpi-hub-card' AS class,
       '#source-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Financial Intensity per Patient' AS title,
       '$' || printf('%,.2f', allowed_per_patient) AS description,
       'Avg. Economic Footprint' AS footer,
       color AS color, 'calculator' AS icon,
       'kpi-hub-card' AS class,
       '#national-kpi-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

-- Second KPI row
SELECT 'card' AS component, 4 AS columns;

SELECT 'Utilization Velocity' AS title,
       printf('%,.2f', services_per_patient) AS description,
       'Procedures per Patient' AS footer,
       color AS color, 'heart-rate-monitor' AS icon,
       'kpi-hub-card' AS class,
       '#procedures-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Geographic Footprint' AS title,
       printf('%,.0f', states_with_data) || ' States' AS description,
       'Active Market Coverage' AS footer,
       color AS color, 'map-pin' AS icon,
       'kpi-hub-card' AS class,
       '#geo-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Market Opportunity Score' AS title,
       ROUND(opportunity_score, 1) || ' / 100' AS description,
       'Strategic Priority Ranking' AS footer,
       color AS color, 'trophy' AS icon,
       'kpi-hub-card' AS class,
       '/mmi/opportunity-scoring.sql' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

SELECT 'Net Federal Reimbursement' AS title,
       '$' || printf('%,.0f', total_medicare_payment) AS description,
       'Actual Medicare Payout' AS footer,
       color AS color, 'receipt' AS icon,
       'kpi-hub-card' AS class,
       '#source-breakdown-section' AS link
FROM mat_condition_national_summary
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition));

-- Narrative after KPI
SELECT 'text' AS component,
       'These indicators establish the scale and intensity of the market, helping identify whether the condition represents a high-volume, high-spend, or high-efficiency opportunity.' AS contents_md;


-- ── Data Source Breakdown ─────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="source-breakdown-section"></div>' AS html;
SELECT 'divider' AS component, 'Source Intensity & Settings Analysis' AS contents;


SELECT 'text' AS component, 'Financial vs. Patient Volume Settings' AS title;
SELECT 'Understanding where dollars flow versus where patients receive care is critical for setting-specific expansion. High spend per data source often signals premium device or specialty outpatient activity.' AS contents;


SELECT 'chart' AS component,
       'Spending Power by Care Setting' AS title,
       'donut' AS type, 5 AS width;
SELECT source_type AS label, total_allowed_amt AS value
FROM mat_condition_source_breakdown
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_allowed_amt DESC;

-- NEW: Beneficiary vs Spend comparison bar (same 7-col slot)
SELECT 'chart' AS component,
       'Patient Reach by Care Setting' AS title,
       'bar' AS type, 7 AS width;
SELECT source_type AS label, total_beneficiaries AS value
FROM mat_condition_source_breakdown
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_beneficiaries DESC;


SELECT 'text' AS component,
       'Comparing spend versus patient distribution helps identify high-cost segments versus high-volume segments, which is critical for targeting interventions or investments.' AS contents_md;

-- ── Top Procedures ────────────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="procedures-section"></div>' AS html;
SELECT 'divider' AS component, 'Clinical Procedure Portfolio' AS contents;

SELECT 'text' AS component, 'High-Impact Clinical Drivers' AS title;
SELECT 'The following table identifies the specific HCPCS codes driving revenue and volume for ' || $condition || '. Target these codes for product development and provider outreach.' AS contents;



SET max_per_page = 10;
SET current_page = COALESCE(CAST($page AS INT), 1);
SET total_rows = (
    SELECT COUNT(*) FROM mat_condition_hcpcs_detail 
    WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
);
SET total_pages = ($total_rows + $max_per_page - 1) / $max_per_page;


SELECT 'table' AS component, 'Primary Revenue Drivers (HCPCS)' AS title, TRUE AS sort, TRUE AS search, 'HCPCS' AS markdown;

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


SELECT 'text' AS component,
       'Focusing on the highest revenue-generating procedures can help prioritize product strategy, provider engagement, and operational improvements.' AS contents_md;

-- ── Geographic Breakdown ──────────────────────────────────────────────────────
SELECT 'html' AS component, '<div id="geo-breakdown-section"></div>' AS html;
SELECT 'divider' AS component, 'Regional Growth Strategy' AS contents;

SELECT 'text' AS component, 'Top Growth Markets by State' AS title;
SELECT 'Geographic intelligence reveals regional "hotspots" with high spending magnitude. Use this data to prioritize regional partnerships and facility investments.' AS contents;


SET geo_max_per_page = 10;
SET geo_current_page = COALESCE(CAST($geo_page AS INT), 1);
SET geo_total_rows = (
    SELECT COUNT(*) FROM mat_condition_state_breakdown 
    WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
);
SET geo_total_pages = ($geo_total_rows + $geo_max_per_page - 1) / $geo_max_per_page;


-- NEW: Top states bar chart
SELECT 'chart' AS component,
       'Top 10 High-Growth States by Spend' AS title,
       'bar' AS type, 12 AS width;
SELECT state_abbr AS label, total_allowed_amt AS value
FROM mat_condition_state_breakdown
WHERE LOWER(TRIM(condition_name)) = LOWER(TRIM($condition))
ORDER BY total_allowed_amt DESC
LIMIT 10;

-- Narrative before table
SELECT 'text' AS component,
       'The table below provides a deeper breakdown of state-level performance, including cost tiers and average spend per patient to identify high-value markets.' AS contents_md;


SELECT 'table' AS component, 
       'State-Level Strategic Matrix' AS title, 
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



SELECT 'text' AS component,
       'States with high spend and high per-patient cost may indicate premium markets, while high-volume, lower-cost regions may present scalability opportunities.' AS contents_md;
```

---

## Executive Insights

```sql mmi/executive-dashboard.sql { route: { caption: "Executive Insights" } }

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '/mmi/home-overview.sql' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
       'Executive Market Intelligence' AS title,
       'Identify high-value disease markets, prioritize growth opportunities, and align strategy using real Medicare data across conditions, geographies, and care settings.' AS description,
       'https://cdn.dribbble.com/userupload/14780232/file/original-264d9812465d5429de36623639ce7115.png?resize=1600x1200&vertical=center' as image,
       'azure' AS color;

SELECT 'text' AS component, 'Portfolio Magnitude Snapshot' AS title;
SELECT 'Our executive suite identifies the intersection of patient reach and financial magnitude to guide high-level investment and partnership decisions across the clinical landscape.' AS contents;

---
--- 4. VIBRANT KPI CARDS (Utilizing CSS Variants)
---
SELECT 'html' AS component, '<div id="kpi-top"></div>' AS html;
SELECT 'divider' AS component, 'Core Performance Indicators' AS contents;
SELECT 'card' AS component, 3 AS columns;

SELECT 'Active Markets' AS title, 
       total_conditions AS description,
       'Verified Clinical Domains' AS footer,
       'teal' AS color, 'activity' AS icon,
       'kpi-card' AS class,
       '#condition-table-section' AS link
FROM mat_executive_kpis;

SELECT 'Total Patient Reach' AS title, 
       printf('%,.1f', total_beneficiaries / 1000000.0) || 'M' AS description,
       'Medicare Part B Beneficiaries' AS footer,
       'azure' AS color, 'users' AS icon,
       'kpi-card kpi-azure' AS class,
       '#condition-table-section' AS link
FROM mat_executive_kpis;

SELECT 'Addressable Market Value' AS title, 
       '$' || printf('%,.1f', total_allowed_amt / 1000000000.0) || 'B' AS description,
       'Total Annual Allowed Spend' AS footer,
       'indigo' AS color, 'currency-dollar' AS icon,
       'kpi-card kpi-indigo' AS class,
       '#condition-table-section' AS link
FROM mat_executive_kpis;

-- Row 2: Operational Depth
SELECT 'card' AS component, 3 AS columns;

SELECT 'Net Revenue Realized' AS title, 
       '$' || printf('%,.1f', total_medicare_payment / 1000000000.0) || 'B' AS description,
       'Actual Medicare Payout' AS footer,
       'teal' AS color, 'receipt' AS icon,
       'kpi-card' AS class,
       '#payment-section' AS link
FROM mat_executive_kpis;

SELECT 'Geographic Footprint' AS title, 
       total_states || ' States' AS description,
       'National Coverage Map' AS footer,
       'blue' AS color, 'map-pin' AS icon,
       'kpi-card kpi-blue' AS class,
       '/mmi/geography.sql' AS link
FROM mat_executive_kpis;

SELECT 'Opportunity Index' AS title, 
       (SELECT COUNT(*) FROM mat_opportunity_score) || ' Targets' AS description,
       'Growth Priority Assets' AS footer,
       'orange' AS color, 'trophy' AS icon,
       'kpi-card kpi-orange' AS class,
       '/mmi/opportunity-scoring.sql' AS link;

-- ── Condition Comparison Charts ────────────────────────────────────────────────
---
--- 5. CATCHY DATA VISUALIZATION (Multicolor Bar Charts)
---
SELECT 'divider' AS component, 'Clinical Market Comparison' AS contents;
SELECT 'text' AS component, 'Benchmarking the Growth Frontier' AS title;
SELECT 'Identifying clinical markets with the highest "Financial Intensity" (spend per patient) vs "Market Reach" (total patients) to dictate your Go-To-Market strategy.' AS contents;

-- Market Value Bar Chart (Azure Theme)
SELECT 'chart' AS component,
       'Total Market Value by Clinical Domain ($)' AS title,
       'bar' AS type, 6 AS width, 'azure' AS color;
SELECT condition_name AS label, total_allowed_amt AS value
FROM mat_condition_national_summary
ORDER BY total_allowed_amt DESC;

-- Patient Reach Bar Chart (Teal Theme)
SELECT 'chart' AS component,
       'Market Reach by Clinical Domain (Beneficiaries)' AS title,
       'bar' AS type, 6 AS width, 'teal' AS color;
SELECT condition_name AS label, total_beneficiaries AS value
FROM mat_condition_national_summary
ORDER BY total_beneficiaries DESC;

SELECT 'text' AS component, 'Executive Insight: Market Magnitude' AS title;
SELECT 'Conditions with the highest total allowed spend represent established revenue pools. However, cross-referencing this with total beneficiaries reveals whether the market is a "High-Volume Utility" or a "Premium Specialty" niche.' AS contents;

-- Service Intensity Bar Chart (Grape/Purple Theme)
SELECT 'chart' AS component,
       'Clinical Intensity (Services per Patient)' AS title,
       'bar' AS type, 6 AS width, 'purple' AS color;
SELECT condition_name AS label, services_per_patient AS value
FROM mat_condition_national_summary
ORDER BY services_per_patient DESC;

-- Financial Intensity Bar Chart (Indigo Theme)
SELECT 'chart' AS component,
       'Economic Density (Spend per Patient)' AS title,
       'bar' AS type, 6 AS width, 'indigo' AS color;
SELECT condition_name AS label, allowed_per_patient AS value
FROM mat_condition_national_summary
ORDER BY allowed_per_patient DESC;

SELECT 'text' AS component, 'Executive Insight: Care Intensity' AS title;
SELECT 'Higher services per patient suggest recurring care cycles, ideal for longitudinal management models. Conversely, high spend per patient often highlights premium care pathways driven by specialized devices or hospital-based interventions.' AS contents;

---
--- 6. REVENUE REALIZATION EFFICIENCY (Success Analytics)
---
SELECT 'html' AS component, '<div id="payment-section"></div>' AS html;
SELECT 'divider' AS component, 'Financial Yield Analysis' AS contents;

SELECT 'text' AS component, 'Efficiency Benchmark: Payment vs. Allowed' AS title;
SELECT 'High ratios indicate high-payout stability, while lower ratios highlight potential reimbursement gaps or pricing pressures. We prioritize clinical domains with efficient realization cycles.' AS contents;

-- Payment Ratio Chart (Orange to indicate revenue alert/insight)
SELECT 'chart' AS component,
       'Revenue Realization Ratio (%)' AS title,
       'bar' AS type, 12 AS width, 'orange' AS color;
SELECT 
    condition_name AS label,
    ROUND(CAST(total_medicare_payment AS REAL) / NULLIF(total_allowed_amt, 0) * 100, 1) AS value
FROM mat_condition_national_summary
ORDER BY value DESC;

---
--- 7. MULTIMODAL CARE SETTINGS (Donut and Stacked)
---
SELECT 'divider' AS component, 'Market Distribution by Care Setting' AS label;

-- Setting Spend (Donut)
SELECT 'chart' AS component,
       'Spend Distribution by Care Channel' AS title,
       'donut' AS type, 6 AS width;
SELECT source_type AS label, SUM(total_allowed_amt) AS value
FROM mat_condition_source_breakdown
GROUP BY source_type
ORDER BY value DESC;

-- Setting Reach (Bar)
SELECT 'chart' AS component,
       'Patient Reach by Care Channel' AS title,
       'bar' AS type, 6 AS width, 'cyan' AS color;
SELECT source_type AS label, SUM(total_beneficiaries) AS value
FROM mat_condition_source_breakdown
GROUP BY source_type
ORDER BY value DESC;

---
--- 8. STRATEGIC PORTFOLIO MATRIX
---
SELECT 'html' AS component, '<div id="condition-table-section"></div>' AS html;
SELECT 'divider' AS component, 'Strategic Market Prioritization Registry' AS contents;

SELECT 'table' AS component, 
       'Market Intelligence Matrix' AS title, 
       TRUE AS sort, TRUE AS search,
       'Condition' AS markdown;

SELECT
    '**[' || condition_name || '](/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') || ')**' AS "Condition",    
    specialty_domain AS "Domain",
    'Tier ' || tier AS "Tier",
    b2b_tier_primary AS "Primary Specialty",
    printf('%,.0f', total_beneficiaries) AS "Patients",
    '$' || printf('%,.0f', total_allowed_amt) AS "Allowed ($)",
    '$' || printf('%,.2f', allowed_per_patient) AS "$/Patient",
    ROUND(opportunity_score, 1) AS "Opp. Score"
FROM mat_condition_national_summary
ORDER BY tier, total_allowed_amt DESC;

---
--- 9. STRATEGIC RECOMMENDATIONS
---
SELECT 'divider' AS component, 'Executive Action Plan' AS label;
SELECT 'text' AS component, 'Recommended Strategic Moves' AS title;
SELECT '
- **Prioritize High-Yield Assets:** Focus on conditions with both high Opportunity Scores and stable Payment Ratios.
- **Scale Population Health:** Target clinical domains with the highest beneficiary reach for broad-market penetration.
- **Niche Specialization:** Identify Tier 1 conditions with high spend per patient to deploy premium specialty solutions.
- **Reimbursement Audit:** Investigate conditions with low payment-to-allowed ratios to identify potential reimbursement friction or market access gaps.
' AS contents_md;

```

---

## Opportunity Scoring

```sql mmi/opportunity-scoring.sql { route: { caption: "Market Prioritization" } }


SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '/mmi/home-overview.sql' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
       'Strategic Opportunity Scoring' AS title,
       'Transforming raw Medicare claims into a prioritized growth roadmap. We balance volume, spend, and strategic alignment to identify your next market win.' AS description,
       'https://images.unsplash.com/photo-1460925895917-afdab827c52f?auto=format&fit=crop&w=1200&q=80' AS image,
       'primary' AS color;


SELECT 'list' AS component, 'The Opportunity Framework' AS title, 'Our composite score (0-100) is engineered to de-risk market entry by weighting three critical pillars:' AS description;
SELECT 'Market Reach (40%)' AS title, 'Total Beneficiary Volume: Identifying where the patients are.' AS description, 'users' AS icon, 'blue' AS color;
SELECT 'Financial Magnitude (40%)' AS title, 'Medicare Allowed Spend: Quantifying the total economic footprint.' AS description, 'currency-dollar' AS icon, 'green' AS color;
SELECT 'Strategic Alignment (20%)' AS title, 'Tier Weighting: Prioritizing clinical domains that match our core competencies.' AS description, 'target' AS icon, 'orange' AS color;

-- ── Top-3 Opportunity KPI Cards (replaces big_number for top picks) ───────────
SELECT 'divider' AS component, 'High-Priority Market Opportunities' AS contents;
SELECT 'card' AS component, 3 AS columns;

SELECT
    '#' || ROW_NUMBER() OVER (ORDER BY opportunity_score DESC) || ' — ' || condition_name AS title,
    'Composite Score: ' || ROUND(opportunity_score, 1) || ' / 100' AS description,
    'Tier ' || tier || ' Focus' AS footer,
    color AS color, icon AS icon,
    'kpi-card' AS class,
    '/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') AS link
FROM mat_opportunity_score
ORDER BY opportunity_score DESC
LIMIT 3;

-- ── Charts ────────────────────────────────────────────────────────────────────
--- 
--- 5. DATA VISUALIZATION & STRATEGIC ANALYSIS
---

SELECT 'text' AS component, 'Market Prioritization Analysis' AS title;
SELECT 'Our Opportunity Magnitude index provides a singular point of truth for market entry. By aggregating multiple performance variables, we rank each clinical condition by its total commercial viability, allowing for a concentrated sales strategy rather than a fragmented approach.' AS contents;

SELECT 'chart' AS component,
       'Opportunity Magnitude by Condition' AS title,
       'bar' AS type, 12 AS width, 
       'Score (0-100)' AS y_title;
SELECT condition_name AS label, opportunity_score AS value, color AS color
FROM mat_opportunity_score
ORDER BY opportunity_score DESC;

-- Divider with Narration for Decomposition
SELECT 'divider' AS component, 'Market Drivers: Volume vs. Spending Power' AS contents;

SELECT 'alert' AS component, 
       'Understanding Market Intensity' AS title,
       'The chart below deconstructs the opportunity score. A high "Financial Magnitude" suggests a premium specialty market, while high "Patient Reach" indicates a high-volume/utility play. The ideal expansion target sits at the intersection of both.' AS description,
       'info' AS color,
       'analyze' AS icon;

SELECT 'chart' AS component, 
       'Market Intensity Decomposition' AS title, 
       'bar' AS type, 12 AS width;

-- Series 1: Normalized Volume
SELECT 
    condition_name AS label,
    ROUND(CAST(total_benes AS REAL) / NULLIF((SELECT MAX(total_benes) FROM mat_opportunity_score), 0) * 40, 1) AS value,
    'Patient Reach (Normalized)' AS series,
    'azure' AS color
FROM mat_opportunity_score;

-- Series 2: Normalized Spend
SELECT 
    condition_name AS label,
    ROUND(CAST(total_allowed AS REAL) / NULLIF((SELECT MAX(total_allowed) FROM mat_opportunity_score), 0) * 40, 1) AS value,
    'Financial Magnitude (Normalized)' AS series,
    'indigo' AS color
FROM mat_opportunity_score;

--- 
--- 6. TACTICAL EXECUTION MATRIX
---

SELECT 'text' AS component, 'Execution Blueprint' AS title;
SELECT 'The following matrix translates our high-level strategy into tactical sales territories. Use the search and sort functions to filter by Primary Specialty or Strategic Tier to align with specific departmental quotas and clinical expertise.' AS contents;

SELECT 'table' AS component, 'Full Market Priority Matrix' AS title, TRUE AS sort, TRUE AS search,
       'Condition' AS markdown;
SELECT
    ROW_NUMBER() OVER (ORDER BY opportunity_score DESC)  AS "Rank",    
    '**[' || condition_name || '](/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') || ')**' AS "Condition",  
    specialty_domain AS "Domain",
    'Tier ' || tier AS "Tier",
    b2b_tier_primary AS "Primary Specialty",
    printf('%,.0f', total_benes) AS "Beneficiaries",
    '$' || printf('%,.0f', total_allowed) AS "Allowed ($)",
    ROUND(opportunity_score, 1) AS "Market Score"
FROM mat_opportunity_score;
```

---

## Clinical Portfolio Directory

```sql mmi/conditions.sql { route: { caption: "Clinical Portfolio" } }
-- @route.description "Clinical Portfolio Registry — Strategic Market Targets"


SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '/mmi/home-overview.sql' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

--
--- 2. SALES HERO: THE CLINICAL PORTFOLIO
---
SELECT 'hero' AS component,
       'Clinical Portfolio Registry' AS title,
       'A centralized catalog of high-value clinical domains. Each entry represents a verified market opportunity anchored in 2023 CMS utilization data. Navigate to any condition to analyze patient density and financial magnitude.' AS description,
       'https://cdn.prod.website-files.com/686d4fca96e4813b96758e56/686d4fca96e4813b9675a490_chronic-disease-registry-provides-data-to-improve-care.jpg' AS image,
       'indigo' AS color;

---
--- 3. STRATEGIC INTEGRITY CALLOUT (Manager Requirement)
---
SELECT 'alert' AS component,
       'Operational Truth: 2023 CMS Clinical Mapping' AS title,
       'Every condition in this registry is mapped to official 2023 CMS ICD-10 and HCPCS standards. This ensures that your clinical portfolio is built on the same verified records used by federal reimbursement agencies.' AS description,
       'info' AS color, 'shield-check' AS icon;

---
--- 4. PORTFOLIO KPI CARDS (Vibrant Sales Formatting)
---
SELECT 'divider' AS component, 'Portfolio Segmentation' AS label;
SELECT 'card' AS component, 4 AS columns;

SELECT 'Total Clinical Targets' AS title,
       COUNT(*) || '' AS description,
       'Active Market Opportunities' AS footer,
       'teal' AS color, 'activity' AS icon,
       'kpi-card' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1;

SELECT 'Tier 1 — High-Yield' AS title,
       COUNT(*) || '' AS description,
       'Flagship Market Opportunities' AS footer,
       'orange' AS color, 'star' AS icon,
       'kpi-card kpi-orange' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1 AND tier = 1;

SELECT 'Tier 2 — Core Growth' AS title,
       COUNT(*) || '' AS description,
       'Strategic Expansion Assets' AS footer,
       'blue' AS color, 'stack-front' AS icon,
       'kpi-card kpi-blue' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1 AND tier = 2;

SELECT 'Tier 3 — Utility' AS title,
       COUNT(*) || '' AS description,
       'Broad-Market Baseline' AS footer,
       'grape' AS color, 'list' AS icon,
       'kpi-card kpi-grape' AS class,
       '#registry-table' AS link
FROM dim_condition_registry WHERE is_active = 1 AND tier = 3;

---
--- 5. STRATEGIC CONDITION BRIEFING (Colorful Cards)
---
SELECT 'divider' AS component, 'High-Impact Clinical Segments' AS label;
SELECT 'text' AS component, 'Targeted Clinical Domains' AS title;
SELECT 'Filter by Strategic Tier or Specialty Domain to align these 2023 market targets with your organizational capabilities and expansion goals.' AS contents;

SELECT 'card' AS component, 3 AS columns;
SELECT
    r.condition_name AS title,
    r.specialty_domain AS subtitle,
    'Strategic Tier ' || r.tier || ' | Primary Target: ' || COALESCE(r.b2b_tier_primary, '—') AS description,
    '/mmi/condition-hub.sql?condition=' || REPLACE(r.condition_name, ' ', '%20') AS link,
    'Analyze Market' AS footer,
    r.icon AS icon,
    r.color AS color
FROM dim_condition_registry r
WHERE r.is_active = 1
ORDER BY r.tier, r.condition_name;

---
--- 6. MARKET INTELLIGENCE REGISTRY (Colorful Table with Badges)
---
SELECT 'html' AS component, '<div id="registry-table"></div>' AS html;
SELECT 'divider' AS component, 'Evidence-Based Registry Table' AS contents;
SELECT 'table' AS component, 'Clinical Portfolio Matrix' AS title, TRUE AS sort, TRUE AS search, TRUE AS markdown;

SELECT
    condition_name  AS "Condition",
    body_system AS "Clinical System",
    tier AS "Tier",
    specialty_domain AS "Strategic Domain",
    b2b_tier_primary AS "B2B Focus",
    icd10_prefix  AS "ICD-10 Base",
    hcpcs_range_start || '–' || hcpcs_range_end AS "Proc. Range",
    CASE use_bygeo WHEN 1 THEN '✓' ELSE '—' END AS "GEO",
    CASE use_dmepos WHEN 1 THEN '✓' ELSE '—' END AS "DME",
    CASE use_hospital WHEN 1 THEN '✓' ELSE '—' END AS "HOSP",
    CASE is_active   WHEN 1 THEN 'Active' ELSE 'Inactive' END AS "Status"
FROM dim_condition_registry
WHERE is_active = 1
ORDER BY tier, condition_name;

---
--- 7. CALL TO ACTION
---
SELECT 'divider' AS component;
SELECT 'text' AS component, 'Next Strategic Move' AS title;
SELECT 'Once you have identified your clinical targets, proceed to the Opportunity Scoring section to rank these conditions by their 2023 revenue realization and patient density.' AS contents;

```

---

## Tactical Analytics

```sql mmi/procedure-drilldown.sql { route: { caption: "Tactical Analytics" } }


SET limit = 20;
SET current_page = COALESCE(CAST(:page AS INT), 1);
SET offset = ($current_page - 1) * $limit;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '/mmi/home-overview.sql' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

---
--- 2. SALES HERO: TACTICAL INTELLIGENCE
---
SELECT 'hero' AS component,
       'Tactical Procedure Intelligence' AS title,
       'Granular HCPCS-level analytics synthesized from audited 2023 CMS Public Use Files. Identify high-value clinical drivers, audit utilization velocity, and optimize your product-to-procedure alignment.' AS description,
       'https://bluebrix.health/wp-content/uploads/2025/05/Banner-Image-for-The-Ultimate-Guide-to-Medical-Coding.png' AS image,
       'orange' AS color;

---
--- 3. OPERATIONAL TRUTH CALLOUT (Manager Requirement)
---
SELECT 'alert' AS component,
       'Operational Truth: 2023 HCPCS Utilization Standards' AS title,
       'Every procedure-level insight is anchored in the verified 2023 CMS records. We provide full traceability from clinical domains down to individual CPT/HCPCS utilization patterns.' AS description,
       'info' AS color, 'shield-check' AS icon;

---
--- 4. VIBRANT KPI GRID (Strategic Magnitude)
---
SELECT 'divider' AS component, 'HCPCS Market Magnitude' AS label;
SELECT 'text' AS component, 'Market Surface Area' AS title, 
       'These indicators define the total addressable surface area for procedural interventions across our tracked 2023 clinical registry.' AS contents;

SELECT 'card' AS component, 4 AS columns;

SELECT 'Clinical Breadth' AS title,
       printf('%,.0f', (SELECT COUNT(DISTINCT hcpcs_code) FROM mat_condition_hcpcs_detail)) AS description,
       'Unique HCPCS Codes' AS footer,
       'orange' AS color, 'clipboard-list' AS icon,
       'kpi-card kpi-orange' AS class,
       '#procedure-table' AS link;

SELECT 'Financial Magnitude' AS title,
       '$' || printf('%,.1f', (SELECT SUM(total_allowed_amt) FROM mat_condition_hcpcs_detail) / 1000000000.0) || 'B' AS description,
       'Total Allowed Amount' AS footer,
       'indigo' AS color, 'currency-dollar' AS icon,
       'kpi-card kpi-indigo' AS class,
       '#procedure-table' AS link;

SELECT 'Service Velocity' AS title,
       printf('%,.1f', (SELECT SUM(total_services) FROM mat_condition_hcpcs_detail) / 1000000.0) || 'M' AS description,
       'Procedures Delivered' AS footer,
       'teal' AS color, 'activity' AS icon,
       'kpi-card' AS class,
       '#procedure-table' AS link;

SELECT 'Patient Reach' AS title,
       printf('%,.1f', (SELECT SUM(total_beneficiaries) FROM mat_condition_hcpcs_detail) / 1000000.0) || 'M' AS description,
       'Unique Beneficiaries' AS footer,
       'azure' AS color, 'users' AS icon,
       'kpi-card kpi-azure' AS class,
       '#procedure-table' AS link;

---
--- 5. VISUAL INTELLIGENCE (Catchy Analytics)
---
SELECT 'divider' AS component, 'Revenue Intensity Analysis' AS contents;

SELECT 'chart' AS component,
       'Top 10 High-Impact Procedures by Spend ($)' AS title,
       'bar' AS type, 12 AS width, 'orange' AS color;
SELECT 
    hcpcs_code || ' • ' || COALESCE(SUBSTR(procedure_description, 1, 40), '') AS label,
    SUM(total_allowed_amt) AS value
FROM mat_condition_hcpcs_detail
WHERE ($condition IS NULL OR TRIM(LOWER(condition_name)) = TRIM(LOWER($condition)))
GROUP BY hcpcs_code, procedure_description
ORDER BY SUM(total_allowed_amt) DESC
LIMIT 10;

SELECT 'text' AS component, 'Executive Insight: Procedural Concentration' AS title,
       'The data above highlights the primary revenue drivers. High concentration in specific HCPCS codes typically indicates a "Flagship" procedure environment, while a broad distribution suggests a multi-modal care pathway.' AS contents;

---
--- 6. CLINICAL DRIVER MATRIX (Table with Full Data)
---
SELECT 'html' AS component, '<div id="procedure-table"></div>' AS html;
SELECT 'divider' AS component, 'Granular Procedure Portfolio' AS contents;

-- Using markdown for links and bolding to ensure the 'SaaS' feel
SELECT 'table' AS component, 
       'Procedure Intelligence Matrix (HCPCS)' AS title, 
       TRUE AS sort, TRUE AS search,
       'Condition' AS markdown,
       'HCPCS' AS markdown;

SELECT
    '' || hcpcs_code || '' AS "HCPCS",
    COALESCE(procedure_description, 'DRG ' || hcpcs_code) AS "Description",
    procedure_category AS "Category",    
    '[' || condition_name || '](/mmi/condition-hub.sql?condition=' || REPLACE(condition_name, ' ', '%20') || ')' AS "Condition",    
    source_type AS "Source",
    printf('%,.0f', total_services) AS "Services",
    printf('%,.0f', total_beneficiaries) AS "Patients",
    '$' || printf('%,.0f', total_allowed_amt) AS "Market Value",
    '$' || printf('%,.2f', avg_allowed_per_service) AS "Val/Service"
FROM mat_condition_hcpcs_detail
WHERE ($condition IS NULL OR TRIM(LOWER(condition_name)) = TRIM(LOWER($condition)))
  AND ($hcpcs IS NULL OR hcpcs_code = $hcpcs)
ORDER BY total_allowed_amt DESC
LIMIT $limit
OFFSET $offset;

---
--- 7. PAGINATION LOGIC (Strictly Preserved)
---
SET total_rows = (SELECT COUNT(*) FROM mat_condition_hcpcs_detail 
    WHERE ($condition IS NULL OR TRIM(LOWER(condition_name)) = TRIM(LOWER($condition))));
SET total_pages = ($total_rows + $limit - 1) / $limit;

SELECT 'pagination' AS component,
    ($current_page <= 1) AS previous_disabled,
    ($current_page >= $total_pages) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page', $current_page - 1, 'condition', $condition)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page', $current_page + 1, 'condition', $condition)) AS next_link;

WITH RECURSIVE page_numbers AS (
    SELECT MAX(1, $current_page - 5) AS n
    UNION ALL
    SELECT n + 1 FROM page_numbers WHERE n < MIN($total_pages, $current_page + 5)
)
SELECT n AS contents,
       sqlpage.link(sqlpage.path(), json_object('page', n, 'condition', $condition)) AS link,
       (n = $current_page) AS active
FROM page_numbers;

---
--- 8. STRATEGIC CALL TO ACTION
---
SELECT 'divider' AS component;
SELECT 'text' AS component, 'Strategic Next Steps' AS title,
       'Validate these procedural drivers against the Geographic Intelligence dashboard to identify regional hotspots for targeted facility investment or sales expansion.' AS contents;
```

---

## Regional Intelligence

```sql mmi/geography.sql { route: { caption: "Regional Intelligence" } }



SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '/mmi/home-overview.sql' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

---
--- 2. SALES HERO: REGIONAL GROWTH VISION
---
SELECT 'hero' AS component,
       'Geographic Market Intelligence' AS title,
       'State-level market sizing, financial cost tiers, and GPCI reimbursement factors synthesized from 2023 CMS datasets. Identify regional hotspots and optimize facility investments with geographic precision.' AS description,
       'https://assignmentpoint.com/wp-content/uploads/2017/07/Health-Geography.jpg' AS image,
       'indigo' AS color;

---
--- 3. OPERATIONAL TRUTH CALLOUT (Manager Requirement)
---
SELECT 'alert' AS component,
       'Operational Truth: 2023 Geographic Benchmarks' AS title,
       'All geographic analytics are anchored in audited 2023 CMS Public Use Files. We provide full transparency into regional GPCI factors and patient density across the national landscape.' AS description,
       'info' AS color, 'shield-check' AS icon;

---
--- 4. VIBRANT KPI GRID (Regional Magnitude)
---
SELECT 'divider' AS component, 'Regional Market Footprint' AS contents;
SELECT 'text' AS component, 'National Reach & Economic Magnitude' AS title, 
       'These indicators define the geographic breadth and financial magnitude of our clinical portfolio across the United States.' AS contents;

SELECT 'card' AS component, 4 AS columns;

SELECT 'Regional Breadth' AS title,
       COUNT(DISTINCT state_abbr) || ' States' AS description,
       'Active Market Coverage' AS footer,
       'indigo' AS color, 'map-pin' AS icon,
       'kpi-card kpi-indigo' AS class,
       '#state-table' AS link
FROM mat_condition_state_breakdown;

SELECT 'Top Market Magnitude' AS title,
       '$' || printf('%,.1f', MAX(state_total) / 1000000000.0) || 'B' AS description,
       'Max State Allowed Spend' AS footer,
       'teal' AS color, 'trending-up' AS icon,
       'kpi-card' AS class,
       '#state-table' AS link
FROM (SELECT state_abbr, SUM(total_allowed_amt) AS state_total FROM mat_condition_state_breakdown GROUP BY state_abbr);

SELECT 'Economic Intensity' AS title,
       '$' || printf('%,.0f', AVG(allowed_per_patient)) AS description,
       'Avg. Allowed per Patient' AS footer,
       'azure' AS color, 'calculator' AS icon,
       'kpi-card kpi-azure' AS class,
       '#state-table' AS link
FROM mat_condition_state_breakdown WHERE allowed_per_patient > 0;

SELECT 'Clinical Breadth' AS title,
       COUNT(DISTINCT condition_name) || ' Clusters' AS description,
       'Clinical Portfolio Covered' AS footer,
       'orange' AS color, 'activity' AS icon,
       'kpi-card kpi-orange' AS class,
       '#state-table' AS link
FROM mat_condition_state_breakdown;

---
--- 5. VISUAL INTELLIGENCE (Regional Comparison)
---
SELECT 'divider' AS component, 'Regional Density And Value Distribution' AS contents;

SELECT 'chart' AS component, 'Top 10 Clinical Markets by Opportunity' AS title, 'treemap' AS type, 8 AS width, TRUE as labels;
SELECT 
    specialty_domain AS series,
    condition_name AS label, 
    opportunity_score AS value
FROM mat_condition_national_summary 
ORDER BY opportunity_score DESC LIMIT 10;

-- Treemap handles the "Dominant Bar" issue by using area proportions.
SELECT 'chart' AS component, 
       'National Market Value Concentration' AS title, 
       'bar' AS type, 
       TRUE AS horizontal,
       TRUE AS stacked,
       12 AS width;

SELECT 
    state_abbr AS series,
    state_abbr AS label, 
    SUM(total_allowed_amt) AS value
FROM mat_condition_state_breakdown 
GROUP BY state_abbr 
ORDER BY value DESC 
LIMIT 20;


-- Donut chart provides a different visual for "Market Share"
SELECT 'chart' AS component,
       'Market Share by Beneficiary Density (Top 10 States)' AS title,
       'donut' AS type, 6 AS width;
SELECT state_abbr AS label, SUM(total_beneficiaries) AS value
FROM mat_condition_state_breakdown
GROUP BY state_abbr
ORDER BY SUM(total_beneficiaries) DESC
LIMIT 10;

-- A colorful bar chart for specific growth comparisons
SELECT 'chart' AS component,
       'Regional Spending Intensity (Allowed $)' AS title,
       'bar' AS type, 6 AS width, 'indigo' AS color;
SELECT state_abbr AS label, SUM(total_allowed_amt) AS value
FROM mat_condition_state_breakdown
GROUP BY state_abbr
ORDER BY SUM(total_allowed_amt) DESC
LIMIT 10;

SELECT 'text' AS component, 'Executive Insight: Regional Alignment' AS title,
       'The Treemap above de-emphasizes outliers to show how the total market value is distributed across regions. High spend combined with high beneficiary density (Donut Chart) indicates a "Core Growth" region, essential for facility-based expansion.' AS contents;

---
--- 6. REGIONAL STRATEGY MATRIX (Table)
---
SELECT 'html' AS component, '<div id="state-table"></div>' AS html;
SELECT 'divider' AS component, 'Strategic Regional Registry' AS contents;

SET max_per_page_obj = 10;
SET count_obj = (SELECT COUNT(DISTINCT state_abbr || '|' || COALESCE(locality_name,'')) FROM mat_condition_state_breakdown);
SET pages_obj = (CAST($count_obj AS INT) / $max_per_page_obj) + (CASE WHEN ($count_obj % $max_per_page_obj) = 0 THEN 0 ELSE 1 END);
SET current_page_obj = COALESCE(CAST($page_obj AS INT), 1);

SELECT 'table' AS component, 'Regional Growth Strategy Matrix' AS title, TRUE AS sort, TRUE AS search;
SELECT
    state_abbr AS "State",
    COALESCE(NULLIF(locality_name, ''), 'Statewide') AS "Market Cluster",
    MAX(cost_tier) AS "Economic Tier",
    MAX(pw_gpci) AS "GPCI Index",
    COUNT(DISTINCT condition_name) AS "Clinical Portfolio",
    printf('%,.0f', SUM(total_beneficiaries)) AS "Patients",
    '$' || printf('%,.0f', SUM(total_allowed_amt)) AS "Allowed Magnitude"
FROM mat_condition_state_breakdown
GROUP BY state_abbr, locality_name
ORDER BY SUM(total_allowed_amt) DESC
LIMIT $max_per_page_obj
OFFSET ($current_page_obj - 1) * $max_per_page_obj;

---
--- 7. PAGINATION LOGIC (Strictly Preserved)
---
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

---
--- 8. CALL TO ACTION
---
SELECT 'divider' AS component;
SELECT 'text' AS component, 'Strategic Next Step' AS title,
       'Correlate these geographic hotspots with the Tactical Procedure Intelligence dashboard to understand which high-value codes are driving spend in your target states.' AS contents;
```

---

## Data Provenance

```sql mmi/data-dictionary.sql { route: { caption: "Data Provenance" } }

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Home' AS title, '/mmi/home-overview.sql' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

---
--- 2. SALES HERO: THE ARCHITECTURE OF TRUST
---
SELECT 'hero' AS component,
       'Data Provenance' AS title,
       'Full transparency into the Medigy clinical pipeline. This directory provides the evidence-base for our analytics, auditing every data source, materialized view, and schema object that powers your growth roadmap.' AS description,
       'https://www.encompasscorporation.com/wp-content/uploads/2025/10/Data-Provenance-Blog-Image-1680x944px-1920x1079.png' AS image,
       'gray' AS color;

---
--- 3. STRATEGIC INTEGRITY CALLOUT (Manager Requirement)
---
SELECT 'alert' AS component,
       'The Gold Standard: 2023 CMS Veracity' AS title,
       'Our "Operational Truth" mandate requires absolute traceability. Every insight displayed across this platform can be audited back to the specific CMS Public Use Files and the transformation logic defined in the schema below.' AS description,
       'info' AS color, 'shield-check' AS icon;

---
--- 4. SCHEMA MAGNITUDE CARDS (Professional Formatting)
---
SELECT 'divider' AS component, 'System Architecture Overview' AS contents;
SELECT 'text' AS component, 'Database Scale & Performance' AS title,
       'Our high-performance schema is optimized for real-time market drilling, synthesizing millions of claims records into actionable materialized views.' AS contents;

SELECT 'card' AS component, 4 AS columns;

SELECT 'Total Schema Objects' AS title, 
       COUNT(*) || ' Tables/Views' AS description,
       'Active Database Inventory' AS footer,
       'blue' AS color, 'database' AS icon,
       'dict-stat-card' AS class,
       '#objects-table' AS link
FROM data_tables_derived;

SELECT 'Materialized Intelligence' AS title,
       COUNT(*) || ' Portfolios' AS description,
       'Optimized Analytics Views' AS footer,
       'teal' AS color, 'table' AS icon,
       'dict-stat-card' AS class,
       '#materialized-table' AS link
FROM data_tables_derived WHERE category = 'Materialized Table';

SELECT 'Core Fact Foundations' AS title,
       COUNT(*) || ' Tables' AS description,
       'Standardized Clinical Facts' AS footer,
       'indigo' AS color, 'layers-linked' AS icon,
       'dict-stat-card' AS class,
       '#objects-table' AS link
FROM data_tables_derived WHERE category = 'Core Fact';

SELECT 'Performance Acceleration' AS title,
       COUNT(*) || ' Indexes' AS description,
       'B-Tree Execution Layer' AS footer,
       'orange' AS color, 'bolt' AS icon,
       'dict-stat-card' AS class,
       '#indexes-table' AS link
FROM data_dictionary_indexes;

---
--- 5. EXTERNAL DATA PROVENANCE (Audit Trail)
---
SELECT 'divider' AS component, 'Verified Data Foundations ' AS contents;
SELECT 'text' AS component, 'External Source Lineage' AS title,
       'We ingest, clean, and standardize the following audited federal data streams to ensure a unified view of the Medicare landscape.' AS contents;

SELECT 'list' AS component;
SELECT 
    title,
    description || 
        CASE 
            WHEN version_year IS NOT NULL 
            THEN ' (Data Year: ' || version_year || ')' 
            ELSE '' 
        END AS description,
    link,
    'external-link' AS icon, 
    'blue' AS color
FROM data_provenance 
WHERE object_type = 'external_source'
ORDER BY version_year DESC, title;

---
--- 6. SCHEMA DATA DICTIONARY (Derived Inventory)
---
SELECT 'html' AS component, '<div id="objects-table"></div>' AS html;
SELECT 'divider' AS component, 'Database Object Dictionary' AS contents;

SET max_per_page_obj = 10;
SET count_obj = (SELECT COUNT(*) FROM data_tables_derived);
SET pages_obj = (CAST($count_obj AS INT) / $max_per_page_obj) + (CASE WHEN ($count_obj % $max_per_page_obj) = 0 THEN 0 ELSE 1 END);
SET current_page_obj = COALESCE(CAST($page_obj AS INT), 1);

SELECT 'table' AS component, 
       'Derived Schema Inventory' AS title,
       TRUE AS sort, TRUE AS search;

SELECT 
    object_name AS "Object Name", 
    object_type AS "Object Type", 
    category AS "Clinical Category"
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

---
--- 7. PERFORMANCE INDEXES (Query Optimization)
---
SELECT 'html' AS component, '<div id="indexes-table"></div>' AS html;
SELECT 'divider' AS component, 'Optimization Layer' AS contents;

SET max_per_page_idx = 10;
SET count_idx = (SELECT COUNT(*) FROM data_dictionary_indexes);
SET pages_idx = (CAST($count_idx AS INT) / $max_per_page_idx) + (CASE WHEN ($count_idx % $max_per_page_idx) = 0 THEN 0 ELSE 1 END);
SET current_page_idx = COALESCE(CAST($page_idx AS INT), 1);

SELECT 'table' AS component, 'Execution Performance Indexes' AS title, TRUE AS hover, TRUE AS striped_rows;
SELECT 
    index_name AS "Index Descriptor",
    table_name AS "Target Entity",
    description AS "Performance Purpose"
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

---
--- 8. MATERIALIZED PORTFOLIO (Intelligence Storage)
---
SELECT 'html' AS component, '<div id="materialized-table"></div>' AS html;
SELECT 'divider' AS component, 'Intelligence Storage Layer' AS contents;

SET max_per_page_mat = 10;
SET count_mat = (SELECT COUNT(*) FROM data_tables_derived WHERE category = 'Materialized Table' );
SET pages_mat = (CAST($count_mat AS INT) / $max_per_page_mat) + (CASE WHEN ($count_mat % $max_per_page_mat) = 0 THEN 0 ELSE 1 END);
SET current_page_mat = COALESCE(CAST($page_mat AS INT), 1);

SELECT 'table' AS component, 'Materialized Growth Portfolios' AS title, TRUE AS hover;
SELECT 
    object_name AS "Table Identifier", 
    'Ready for Growth Briefing' AS "Status"
FROM data_tables_derived
 WHERE category = 'Materialized Table' 
ORDER BY object_name 
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
