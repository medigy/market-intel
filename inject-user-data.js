// inject-user-data.js
// =====================================================
// Medigy Opportunity Atlas — User Data Injection
// BACKUP MECHANISM - sets body data attributes
// =====================================================

(function () {
  'use strict';

  function getCookie(name) {
    const nameEQ = encodeURIComponent(name) + "=";
    const cookies = document.cookie.split(';');
    for (let cookie of cookies) {
      cookie = cookie.trim();
      if (cookie.startsWith(nameEQ)) {
        try {
          return decodeURIComponent(cookie.substring(nameEQ.length));
        } catch (e) {
          return null;
        }
      }
    }
    return null;
  }

  const profileCookie = getCookie('medigy_moa_registration_profile_v2');

  if (profileCookie && profileCookie.trim().startsWith('{')) {
    try {
      const profile = JSON.parse(profileCookie);

      // Set body attributes as backup
      document.body.dataset.userEmail = profile.emailAddress || profile.email || '';
      document.body.dataset.userId = profile.user_id || profile.userId || '';
      document.body.dataset.userName = profile.firstName ? `${profile.firstName} ${profile.lastName}` : '';

      console.log('[DataInjector] ✅ Set body attributes:', {
        email: document.body.dataset.userEmail
      });
    } catch (e) {
      console.error('[DataInjector] Failed to parse cookie:', e);
    }
  }
})();