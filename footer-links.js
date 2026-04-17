(() => {
  const COOKIE_NAME = 'medigy_mmi_registration_profile_v2';
  const SKIP_FOR_NOW_PARAM = 'skip_for_now';
  const SKIP_SESSION_KEY = 'mmi_registration_skip_once';
  const ROUTE_SUFFIX_REGISTRATION = '/';
  const ROUTE_SUFFIX_REGISTRATION_INDEX = '/index.sql';
  const ROUTE_SUFFIX_REGISTRATION_ALIAS = '/registration.sql';
  const ROUTE_SUFFIX_REGISTRATION_SUBMIT = '/registration-submit.sql';
  const ROUTE_SUFFIX_HOME = '/mmi/home-overview.sql';

  const normalizePath = (pathValue) => {
    const asString = String(pathValue || '').trim();
    if (!asString) {
      return '/';
    }
    const trimmed = asString.length > 1 ? asString.replace(/\/+$/, '') : asString;
    return trimmed || '/';
  };

  const resolveBasePath = (pathname) => {
    const normalized = normalizePath(pathname);

    if (normalized.includes('/mmi/')) {
      return normalized.split('/mmi/')[0] || '';
    }

    const knownRouteSuffixes = [
      ROUTE_SUFFIX_REGISTRATION_INDEX,
      ROUTE_SUFFIX_REGISTRATION_ALIAS,
      ROUTE_SUFFIX_REGISTRATION_SUBMIT
    ];

    for (const routeSuffix of knownRouteSuffixes) {
      if (normalized.endsWith(routeSuffix)) {
        return normalized.slice(0, -routeSuffix.length);
      }
    }

    if (normalized === '/') {
      return '';
    }

    return normalized;
  };

  const BASE_PATH = resolveBasePath(window.location.pathname || '/');

  const buildPath = (routeSuffix) => {
    if (routeSuffix === ROUTE_SUFFIX_REGISTRATION) {
      return BASE_PATH ? `${BASE_PATH}/` : '/';
    }
    return `${BASE_PATH}${routeSuffix}`;
  };

  const REGISTRATION_PATH = buildPath(ROUTE_SUFFIX_REGISTRATION);
  const REGISTRATION_INDEX_PATH = buildPath(ROUTE_SUFFIX_REGISTRATION_INDEX);
  const REGISTRATION_ALIAS_PATH = buildPath(ROUTE_SUFFIX_REGISTRATION_ALIAS);
  const REGISTRATION_SUBMIT_PATH = buildPath(ROUTE_SUFFIX_REGISTRATION_SUBMIT);
  const HOME_PAGE_PATH = buildPath(ROUTE_SUFFIX_HOME);

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

  const hasSkippedRegistrationForSession = () => {
    try {
      return window.sessionStorage.getItem(SKIP_SESSION_KEY) === 'true';
    } catch {
      return false;
    }
  };

  const markSkippedRegistrationForSession = () => {
    try {
      window.sessionStorage.setItem(SKIP_SESSION_KEY, 'true');
    } catch {
      // Ignore sessionStorage errors and fallback to standard registration flow.
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

  const normalizedPath = normalizePath(window.location.pathname || '/');
  const searchParams = new URLSearchParams(window.location.search || '');
  const isRegistrationPage =
    normalizedPath === normalizePath(REGISTRATION_PATH) ||
    normalizedPath === normalizePath(REGISTRATION_INDEX_PATH) ||
    normalizedPath === normalizePath(REGISTRATION_ALIAS_PATH);
  const hasSubmittedRegistrationParams = () => {
    return Boolean(
      String(searchParams.get('first_name') || '').trim() &&
      String(searchParams.get('last_name') || '').trim() &&
      String(searchParams.get('email_address') || '').trim() &&
      String(searchParams.get('consent_acknowledged') || '').trim()
    );
  };

  const hasSkipForNowParam =
    String(searchParams.get(SKIP_FOR_NOW_PARAM) || '').trim() === '1';

  const persistSubmittedRegistration = () => {
    const payload = {
      firstName: String(searchParams.get('first_name') || '').trim(),
      secondName: String(searchParams.get('last_name') || '').trim(),
      fullName: `${String(searchParams.get('first_name') || '').trim()} ${String(searchParams.get('last_name') || '').trim()}`.trim(),
      emailAddress: String(searchParams.get('email_address') || '').trim(),
      phoneNumber: String(searchParams.get('phone_number') || '').trim()
        ? normalizePhoneNumberWithCountryCode(String(searchParams.get('phone_number') || '').trim())
        : '',
      organization: String(searchParams.get('organization') || '').trim(),
      purposeOfVisit: String(searchParams.get('purpose_of_visit') || '').trim(),
      consentAcknowledged: String(searchParams.get('consent_acknowledged') || '').trim().toLowerCase(),
      registeredAt: new Date().toISOString()
    };

    if (!payload.firstName || !payload.secondName || !payload.emailAddress || !payload.consentAcknowledged) {
      return false;
    }

    if (!isValidEmail(payload.emailAddress)) {
      window.location.replace(`${REGISTRATION_PATH}?error=invalid_email`);
      return false;
    }

    if (payload.phoneNumber && !isValidPhoneNumberWithCountryCode(payload.phoneNumber)) {
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

  const addInlineSkipButtonNextToContinue = () => {
    const formElement = document.querySelector('form');
    if (!formElement) {
      return;
    }

    const submitButton = formElement.querySelector('button[type="submit"], input[type="submit"]');
    if (!submitButton) {
      return;
    }

    const submitContainer = submitButton.parentElement;
    if (!submitContainer || submitContainer.querySelector('[data-mmi-skip-inline="true"]')) {
      return;
    }

    const inlineSkipLink = document.createElement('a');
    inlineSkipLink.textContent = 'Skip for Now';
    inlineSkipLink.href = `${REGISTRATION_PATH}?skip_for_now=1`;
    inlineSkipLink.setAttribute('data-mmi-skip-inline', 'true');
    inlineSkipLink.className = 'btn btn-outline-secondary';

    const tabSpace = document.createTextNode('\u00A0\u00A0\u00A0\u00A0');

    submitContainer.appendChild(tabSpace);
    submitContainer.appendChild(inlineSkipLink);
  };

  const runOnReady = (handler) => {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', handler, { once: true });
      return;
    }
    handler();
  };

  if (isRegistrationPage && hasSubmittedRegistrationParams() && persistSubmittedRegistration()) {
    const browserUserAgent = encodeURIComponent(navigator.userAgent || 'N/A');
    const rawPhone = String(searchParams.get('phone_number') || '').trim();
    const normalizedPhone = rawPhone ? normalizePhoneNumberWithCountryCode(rawPhone) : '';
    window.location.replace(
      `${REGISTRATION_SUBMIT_PATH}?first_name=${encodeURIComponent(String(searchParams.get('first_name') || '').trim())}&last_name=${encodeURIComponent(String(searchParams.get('last_name') || '').trim())}&email_address=${encodeURIComponent(String(searchParams.get('email_address') || '').trim())}&phone_number=${encodeURIComponent(normalizedPhone)}&organization=${encodeURIComponent(String(searchParams.get('organization') || '').trim())}&purpose_of_visit=${encodeURIComponent(String(searchParams.get('purpose_of_visit') || '').trim())}&consent_acknowledged=${encodeURIComponent(String(searchParams.get('consent_acknowledged') || '').trim().toLowerCase())}&user_agent=${browserUserAgent}&ip_address=${encodeURIComponent(String(searchParams.get('ip_address') || '').trim())}`
    );
    return;
  }

  if (isRegistrationPage && hasSkipForNowParam) {
    markSkippedRegistrationForSession();
    window.location.replace(HOME_PAGE_PATH);
    return;
  }

  const hasCookie = hasRegistrationCookie();
  const hasSkippedForSession = hasSkippedRegistrationForSession();

  if (!isRegistrationPage && !hasCookie && !hasSkippedForSession) {
    window.location.replace(REGISTRATION_PATH);
    return;
  }

  if (isRegistrationPage && (hasCookie || hasSkippedForSession)) {
    window.location.replace(HOME_PAGE_PATH);
    return;
  }

  if (!isRegistrationPage) {
    return;
  }

  runOnReady(addInlineSkipButtonNextToContinue);
})();