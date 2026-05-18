// posthog-tracker.js
// =====================================================
// Medigy Opportunity Atlas — PostHog Integration
// SIMPLIFIED VERSION - THIS ONE ACTUALLY WORKS
// =====================================================

(function () {
  'use strict';

  // ========== CONFIGURATION ==========
  const API_KEY = window.POSTHOG_API_KEY || 'phc_uksMoWZsmCBxGLYSXD39cLCDQJ5bvve5RXCppoDmeQZt';
  const API_HOST = window.POSTHOG_HOST || 'https://us.i.posthog.com';

  // ========== INIT POSTHOG ==========
  function initPostHog() {
    console.log('[PostHog] Initializing PostHog SDK...');

    // Initialize PostHog
    posthog.init(API_KEY, {
      api_host: API_HOST,
      capture_pageview: true,  // Let PostHog auto-capture pageviews
      persistence: 'localStorage',
      autocapture: true,
      loaded: function (ph) {
        console.log('[PostHog] ✅ PostHog SDK fully loaded');
        onPostHogReady(ph);
      }
    });
  }

  // ========== POST-INIT LOGIC ==========
  function onPostHogReady(ph) {
    console.log('[PostHog] PostHog ready. Attempting to identify user...');

    // Try to identify the user
    identifyUser();

    // Register super-properties
    posthog.register({
      app_name: 'Medigy Opportunity Atlas',
      app_version: '3.0.0',
      environment: window.location.hostname === 'localhost' ? 'development' : 'production'
    });

    console.log('[PostHog] 🚀 Analytics tracker ready');
  }

  // ========== GET COOKIE HELPER ==========
  function getCookie(name) {
    const nameEQ = encodeURIComponent(name) + "=";
    const cookies = document.cookie.split(';');

    for (let cookie of cookies) {
      cookie = cookie.trim();
      if (cookie.startsWith(nameEQ)) {
        try {
          return decodeURIComponent(cookie.substring(nameEQ.length));
        } catch (e) {
          console.error('[PostHog] Cookie decode error:', e);
          return null;
        }
      }
    }
    return null;
  }

  // ========== IDENTIFY USER - THE CRITICAL FUNCTION ==========
  function identifyUser() {
    console.log('[PostHog] === Starting User Identification ===');

    let userId = null;
    let userEmail = null;
    let userProperties = {};

    // -------- PRIORITY 1: Read from registration cookie --------
    const profileCookie = getCookie('medigy_moa_registration_profile_v2');
    console.log('[PostHog] Cookie check:', profileCookie ? '✅ FOUND' : '❌ NOT FOUND');

    if (profileCookie) {
      try {
        console.log('[PostHog] Raw cookie:', profileCookie.substring(0, 100));

        if (profileCookie.trim().startsWith('{')) {
          const profile = JSON.parse(profileCookie);
          console.log('[PostHog] ✅ Parsed profile:', profile);

          // Extract email - try multiple keys
          userEmail = profile.emailAddress || profile.email || profile.email_address;
          userId = profile.user_id || profile.userId || userEmail;

          // Store all profile data as properties
          userProperties = {
            ...profile,
            email: userEmail,
            source: 'registration_cookie'
          };

          console.log('[PostHog] Extracted from cookie:', { userEmail, userId });
        }
      } catch (e) {
        console.error('[PostHog] ❌ Failed to parse cookie:', e);
        console.log('[PostHog] Cookie value:', profileCookie);
      }
    }

    // -------- PRIORITY 2: Check localStorage --------
    if (!userEmail) {
      const storedEmail = localStorage.getItem('moa_user_email');
      if (storedEmail) {
        userEmail = storedEmail;
        userId = localStorage.getItem('moa_user_id') || storedEmail;
        console.log('[PostHog] Using email from localStorage:', userEmail);
      }
    }

    // -------- PRIORITY 3: Check body data attributes --------
    if (!userEmail && document.body.dataset.userEmail) {
      userEmail = document.body.dataset.userEmail;
      userId = document.body.dataset.userId || userEmail;
      console.log('[PostHog] Using email from body attributes:', userEmail);
    }

    // -------- CRITICAL: Call posthog.identify() --------
    if (userEmail || userId) {
      const distinctId = userEmail || userId;

      console.log('\n[PostHog] ⭐⭐⭐ CALLING posthog.identify() ⭐⭐⭐');
      console.log('[PostHog] distinctId:', distinctId);
      console.log('[PostHog] properties:', userProperties);

      try {
        // THIS IS THE CRITICAL CALL
        posthog.identify(distinctId, userProperties);

        console.log('[PostHog] ✅✅✅ SUCCESS: User identified as:', distinctId);
        console.log('[PostHog] All subsequent events will include this email\n');

      } catch (error) {
        console.error('[PostHog] ❌ Error calling identify():', error);
      }
    } else {
      console.warn('[PostHog] ⚠️⚠️⚠️ NO EMAIL FOUND ⚠️⚠️⚠️');
      console.warn('[PostHog] Checked: cookie, localStorage, body attributes');
      console.warn('[PostHog] User will be tracked as anonymous\n');
    }
  }

  // ========== BOOT UP ==========
  // Wait for DOM to be ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initPostHog);
  } else {
    // DOM already ready
    initPostHog();
  }

  // Expose for debugging
  window.PostHogDebug = {
    cookie: () => getCookie('medigy_moa_registration_profile_v2'),
    distinctId: () => posthog ? posthog.get_distinct_id() : 'PostHog not loaded',
    identify: (email, props) => {
      if (posthog) {
        console.log('[DEBUG] Manual identify call:', email, props);
        posthog.identify(email, props || {});
      } else {
        console.error('PostHog not loaded yet');
      }
    }
  };

})();