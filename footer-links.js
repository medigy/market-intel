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
      String(searchParams.get('email_address') || '').trim()
    );
  };

  const persistSubmittedRegistration = () => {
    const payload = {
      firstName: String(searchParams.get('first_name') || '').trim(),
      secondName: String(searchParams.get('second_name') || '').trim(),
      emailAddress: String(searchParams.get('email_address') || '').trim(),
      organization: String(searchParams.get('organization') || '').trim(),
      message: String(searchParams.get('message') || '').trim(),
      registeredAt: new Date().toISOString()
    };

    if (!payload.firstName || !payload.secondName || !payload.emailAddress) {
      return false;
    }

    const cookieValue = encodeURIComponent(JSON.stringify(payload));
    storeRegistration(cookieValue);
    return true;
  };

  if (isRegistrationPage && hasSubmittedRegistrationParams() && persistSubmittedRegistration()) {
    window.location.replace(
      `${REGISTRATION_SUBMIT_PATH}?first_name=${encodeURIComponent(String(searchParams.get('first_name') || '').trim())}&second_name=${encodeURIComponent(String(searchParams.get('second_name') || '').trim())}&email_address=${encodeURIComponent(String(searchParams.get('email_address') || '').trim())}&organization=${encodeURIComponent(String(searchParams.get('organization') || '').trim())}&message=${encodeURIComponent(String(searchParams.get('message') || '').trim())}`
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
