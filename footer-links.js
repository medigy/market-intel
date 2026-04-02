(() => {
  const COOKIE_NAME = 'medigy_mmi_registration_profile_v2';
  const REGISTRATION_PATH = '/';
  const REGISTRATION_INDEX_PATH = '/index.sql';
  const REGISTRATION_ALIAS_PATH = '/registration.sql';
  const REGISTRATION_SUBMIT_PATH = '/registration-submit.sql';
  const HOME_PAGE_PATH = '/mmi/home-overview.sql';

  const getCookie = (name) => {
    const key = `${name}=`;
    const pairs = document.cookie.split(';');
    for (const pair of pairs) {
      const value = pair.trim();
      if (value.startsWith(key)) {
        return value.substring(key.length);
      }
    }
    return '';
  };

  const getStoredRegistration = () => {
    const cookieValue = getCookie(COOKIE_NAME);
    if (cookieValue) {
      return cookieValue;
    }

    try {
      return window.localStorage.getItem(COOKIE_NAME) || '';
    } catch {
      return '';
    }
  };

  const storeRegistration = (encodedPayload) => {
    document.cookie = `${COOKIE_NAME}=${encodedPayload}; Max-Age=31536000; Path=/; SameSite=Lax`;

    try {
      window.localStorage.setItem(COOKIE_NAME, encodedPayload);
    } catch {
      // Ignore localStorage errors and rely on the cookie.
    }
  };

  const isValidEmail = (email) =>
    /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

  const normalizePhoneNumberWithCountryCode = (phoneNumber) => {
    const trimmedPhoneNumber = String(phoneNumber || '').trim();
    if (!trimmedPhoneNumber.startsWith('+')) {
      return trimmedPhoneNumber.replace(/\D/g, '');
    }
    return `+${trimmedPhoneNumber.slice(1).replace(/\D/g, '')}`;
  };

  const isValidPhoneNumberWithCountryCode = (phoneNumber) =>
    /^\+\d{6,15}$/.test(phoneNumber);

  const hasRegistrationCookie = () => {
    const rawValue = getStoredRegistration();
    if (!rawValue) {
      return false;
    }

    try {
      const parsed = JSON.parse(decodeURIComponent(rawValue));
      return Boolean(
        parsed &&
        String(parsed.firstName || '').trim() &&
        String(parsed.secondName || '').trim() &&
        String(parsed.emailAddress || '').trim()
      );
    } catch {
      return false;
    }
  };

  const rawPath = window.location.pathname || '/';
  const normalizedPath = rawPath.length > 1 ? rawPath.replace(/\/+$/, '') : rawPath;
  const searchParams = new URLSearchParams(window.location.search || '');
  const isRegistrationPage =
    normalizedPath === REGISTRATION_PATH ||
    normalizedPath === REGISTRATION_INDEX_PATH ||
    normalizedPath === REGISTRATION_ALIAS_PATH;
  const hasSubmittedRegistrationParams = () => {
    return Boolean(
      String(searchParams.get('first_name') || '').trim() &&
      String(searchParams.get('second_name') || '').trim() &&
      String(searchParams.get('email_address') || '').trim() &&
      String(searchParams.get('phone_number') || '').trim() &&
      String(searchParams.get('consent_acknowledged') || '').trim()
    );
  };

  const persistSubmittedRegistration = () => {
    const payload = {
      firstName: String(searchParams.get('first_name') || '').trim(),
      secondName: String(searchParams.get('second_name') || '').trim(),
      fullName: `${String(searchParams.get('first_name') || '').trim()} ${String(searchParams.get('second_name') || '').trim()}`.trim(),
      emailAddress: String(searchParams.get('email_address') || '').trim(),
      phoneNumber: normalizePhoneNumberWithCountryCode(String(searchParams.get('phone_number') || '+1').trim()),
      organization: String(searchParams.get('organization') || '').trim(),
      purposeOfVisit: String(searchParams.get('purpose_of_visit') || '').trim(),
      consentAcknowledged: String(searchParams.get('consent_acknowledged') || '').trim().toLowerCase(),
      registeredAt: new Date().toISOString()
    };

    if (!payload.firstName || !payload.secondName || !payload.emailAddress || !payload.phoneNumber || !payload.consentAcknowledged) {
      return false;
    }

    if (!isValidEmail(payload.emailAddress)) {
      window.location.replace(`${REGISTRATION_PATH}?error=invalid_email`);
      return false;
    }

    if (!isValidPhoneNumberWithCountryCode(payload.phoneNumber)) {
      window.location.replace(`${REGISTRATION_PATH}?error=invalid_phone`);
      return false;
    }

    if (!['yes', 'on', 'true', '1'].includes(payload.consentAcknowledged)) {
      window.location.replace(`${REGISTRATION_PATH}?error=invalid_consent`);
      return false;
    }

    const cookieValue = encodeURIComponent(JSON.stringify(payload));
    storeRegistration(cookieValue);
    return true;
  };

  if (isRegistrationPage && hasSubmittedRegistrationParams() && persistSubmittedRegistration()) {
    const browserUserAgent = encodeURIComponent(navigator.userAgent || 'N/A');
    window.location.replace(
      `${REGISTRATION_SUBMIT_PATH}?first_name=${encodeURIComponent(String(searchParams.get('first_name') || '').trim())}&second_name=${encodeURIComponent(String(searchParams.get('second_name') || '').trim())}&email_address=${encodeURIComponent(String(searchParams.get('email_address') || '').trim())}&phone_number=${encodeURIComponent(normalizePhoneNumberWithCountryCode(String(searchParams.get('phone_number') || '+1').trim()))}&organization=${encodeURIComponent(String(searchParams.get('organization') || '').trim())}&purpose_of_visit=${encodeURIComponent(String(searchParams.get('purpose_of_visit') || '').trim())}&consent_acknowledged=${encodeURIComponent(String(searchParams.get('consent_acknowledged') || '').trim().toLowerCase())}&user_agent=${browserUserAgent}&ip_address=${encodeURIComponent(String(searchParams.get('ip_address') || '').trim())}`
    );
    return;
  }

  const hasCookie = hasRegistrationCookie();

  if (!isRegistrationPage && !hasCookie) {
    window.location.replace(REGISTRATION_PATH);
    return;
  }

  if (isRegistrationPage && hasCookie) {
    window.location.replace(HOME_PAGE_PATH);
    return;
  }

  if (!isRegistrationPage) {
    return;
  }
})();
