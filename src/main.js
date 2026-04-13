/**
 * ═══════════════════════════════════════════════════════════════════════════
 *  LinkedIn SalesNav → Public Company Scraper  |  Apify Actor  v2
 *  Node 18 · Educational use only
 * ═══════════════════════════════════════════════════════════════════════════
 *
 *  FIX SUMMARY (v2):
 *  ─────────────────
 *  v1 scraped linkedin.com/search/results/companies/ HTML — but LinkedIn
 *  now renders that page client-side (React), so cheerio sees an empty shell
 *  and finds 0 company links every time.
 *
 *  v2 calls LinkedIn's internal Voyager JSON API instead:
 *    GET /voyager/api/search/blended?keywords=…&filters=…
 *  This endpoint returns structured JSON (no JS required) and is the same
 *  API the browser calls. Each page yields up to 10 company objects with
 *  name, slug, headcount, industry, description, etc — no second scrape needed
 *  for basic data.
 *
 *  We still optionally deep-scrape /company/<slug>/about/ for richer data
 *  (website, specialties, founded date) when deepScrape=true.
 *
 *  PROXY NOTE (free tier):
 *  ─────────────────────────
 *  Free Apify accounts only get DATACENTER proxies. We detect this and fall
 *  back gracefully. Residential proxies have a much higher LinkedIn success
 *  rate but require a paid plan.
 * ═══════════════════════════════════════════════════════════════════════════
 */

import { Actor } from 'apify';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

// ─────────────────────────────────────────────────────────────────────────────
//  LOOKUP TABLES (unchanged from v1)
// ─────────────────────────────────────────────────────────────────────────────

const HEADCOUNT_LABELS = {
  A: '1-10', B: '11-50', C: '51-200', D: '51-200',
  E: '201-500', F: '501-1000', G: '1001-5000', H: '5001-10000', I: '10001+',
};

// SalesNav headcount ID → Voyager facet value (LinkedIn uses same letters)
const HEADCOUNT_TO_VOYAGER = {
  A: 'A', B: 'B', C: 'C', D: 'C', E: 'D', F: 'E', G: 'F', H: 'G', I: 'H',
};

const INDUSTRY_MAP = {
  '1': 'Defense and Space Manufacturing', '3': 'Computer Hardware Manufacturing',
  '4': 'Software Development', '5': 'Computer Networking Products',
  '6': 'Technology, Information and Internet', '7': 'Telecommunications',
  '8': 'Semiconductor Manufacturing', '9': 'Computers and Electronics Manufacturing',
  '10': 'Transportation Equipment Manufacturing', '11': 'Higher Education',
  '12': 'Primary and Secondary Education', '13': 'Education Administration Programs',
  '14': 'Research Services', '15': 'Armed Forces', '16': 'Legislative Offices',
  '17': 'Administration of Justice', '18': 'International Affairs',
  '19': 'Government Administration', '20': 'Executive Offices',
  '21': 'Law Practice', '22': 'Legal Services', '23': 'Management Consulting',
  '24': 'IT Services and IT Consulting', '25': 'Facilities Services',
  '26': 'Civil Engineering', '27': 'Architecture and Planning',
  '28': 'Construction', '29': 'Wholesale Building Materials', '30': 'Real Estate',
  '34': 'Food and Beverage Manufacturing', '36': 'Beverage Manufacturing',
  '41': 'Medical Practices', '42': 'Hospitals and Health Care',
  '43': 'Pharmaceutical Manufacturing', '44': 'Biotechnology Research',
  '46': 'Medical Equipment Manufacturing', '47': 'Embedded Systems',
  '52': 'Aviation and Aerospace Component Manufacturing', '53': 'Airlines and Aviation',
  '60': 'Chemicals Manufacturing', '63': 'Entertainment Providers',
  '71': 'Advertising Services', '72': 'Marketing Services',
  '73': 'Public Relations and Communications Services', '82': 'Oil and Gas',
  '86': 'Strategic Management Services', '87': 'Business Consulting and Services',
  '88': 'Human Resources Services', '89': 'Staffing and Recruiting',
  '91': 'Financial Services', '92': 'Investment Banking',
  '93': 'Investment Management', '94': 'Accounting', '95': 'Insurance',
  '96': 'Banking', '97': 'Venture Capital and Private Equity Principals',
  '104': 'Civic and Social Organizations', '105': 'Non-profit Organizations',
  '110': 'Wellness and Fitness Services', '116': 'Design Services',
  '121': 'E-Learning Providers', '122': 'Computer Games',
  '124': 'Online Audio and Video Media', '125': 'Internet Marketplace Platforms',
  '126': 'IT System Data Services', '127': 'Internet Publishing',
  '129': 'Mental Health Care', '133': 'Renewable Energy Semiconductor Manufacturing',
  '134': 'Electric Power Generation', '136': 'Utilities',
  '138': 'Data Infrastructure and Analytics', '139': 'Computer and Network Security',
  '140': 'Artificial Intelligence', '142': 'Information Services',
  '144': 'Cybersecurity',
};

const REVENUE_CURRENCY_MAP = {
  '1': 'USD', '2': 'EUR', '3': 'GBP', '4': 'INR', '5': 'CAD', '6': 'AUD',
  '7': 'CNY', '8': 'JPY', '9': 'BRL', '10': 'MXN', '11': 'SGD', '12': 'CHF',
};

const COMPANY_TYPE_MAP = {
  'C': 'Public Company', 'D': 'Privately Held', 'E': 'Non-profit',
  'B': 'Partnership', 'A': 'Self-Employed', 'F': 'Government Agency',
  'G': 'Educational Institution', 'H': 'Sole Proprietorship',
};

const HEADCOUNT_GROWTH_MAP = {
  'ABOVE_40': 'Above 40% growth', 'BETWEEN_20_AND_40': '20%-40% growth',
  'BETWEEN_10_AND_20': '10%-20% growth', 'BETWEEN_5_AND_10': '5%-10% growth',
  'BETWEEN_0_AND_5': '0%-5% growth', 'NEGATIVE': 'Negative growth (decline)',
};

const FORTUNE_MAP = { '10': 'Fortune 10', '50': 'Fortune 50', '100': 'Fortune 100', '500': 'Fortune 500' };
const FOLLOWERS_MAP = {
  'A': '1-100', 'B': '101-1K', 'C': '1K-5K', 'D': '5K-10K', 'E': '10K-50K', 'F': '50K+',
};

// Realistic browser user-agents
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
];

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 1 ── DECODE SALES NAVIGATOR URL  (same as v1, kept intact)
// ─────────────────────────────────────────────────────────────────────────────

function safeDecodeUri(s) {
  if (!s) return s;
  try { return decodeURIComponent(s).replace(/\+/g, ' '); }
  catch { return s.replace(/\+/g, ' '); }
}

function parseValueEntry(entry) {
  const obj = { id: null, text: null, selectionType: 'INCLUDED' };
  const idM  = entry.match(/(?:^|,)id:([^,)]+)/);
  const txtM = entry.match(/(?:^|,)text:([^,(]+)/);
  const selM = entry.match(/selectionType:(\w+)/);
  if (idM)  obj.id            = safeDecodeUri(idM[1].trim());
  if (txtM) obj.text          = safeDecodeUri(txtM[1].trim());
  if (selM) obj.selectionType = selM[1];
  return obj;
}

function extractValues(block) {
  const values = [];
  let depth = 0, start = -1;
  for (let i = 0; i < block.length; i++) {
    if (block[i] === '(') { depth++; if (depth === 1) start = i; }
    else if (block[i] === ')') {
      depth--;
      if (depth === 0 && start !== -1) {
        const entry = block.slice(start + 1, i);
        const parsed = parseValueEntry(entry);
        if (parsed.id) values.push(parsed);
        start = -1;
      }
    }
  }
  return values;
}

function decodeSalesNavUrl(rawUrl) {
  let decoded;
  try { decoded = decodeURIComponent(rawUrl); } catch { decoded = rawUrl; }

  const result = {
    keywords: null, savedSearchId: null, sessionId: null, searchType: null,
    headcounts: [], headcountGrowth: [], industries: [], regions: [],
    companyTypes: [], revenue: { min: null, max: null, currency: 'USD' },
    fortune: [], followers: [], technologiesUsed: [], departmentHeadcount: [],
    departmentGrowth: [], spotlight: [], currentCompany: [], pastCompany: [],
    postalCodes: [], jobTitles: [], seniorityLevels: [], functions: [],
    yearsInRole: [], yearsAtCompany: [], yearsOfExperience: [], schools: [],
    groups: [], profileLanguages: [], rawDecoded: decoded, unknownFilters: [],
  };

  if (decoded.includes('/sales/search/company'))      result.searchType = 'company';
  else if (decoded.includes('/sales/search/people'))  result.searchType = 'people';

  const kwM = decoded.match(/[,(?]keywords:([^,)&\n]+)/);
  if (kwM) result.keywords = safeDecodeUri(kwM[1].trim());

  const savedM = decoded.match(/savedSearchId[=:](\d+)/);
  if (savedM) result.savedSearchId = savedM[1];

  const sessionM = decoded.match(/sessionId[=:]([^&)\n]+)/);
  if (sessionM) result.sessionId = sessionM[1];

  // Revenue
  const revM = decoded.match(
    /type:ANNUAL_REVENUE[^)]*rangeValue:\(min:(\d+(?:\.\d+)?),max:(\d+(?:\.\d+)?)\)(?:[^)]*selectedSubFilter:(\d+))?/
  );
  if (revM) {
    result.revenue.min      = parseFloat(revM[1]);
    result.revenue.max      = parseFloat(revM[2]);
    result.revenue.currency = REVENUE_CURRENCY_MAP[revM[3]] || 'USD';
  }
  const revMinOnly = decoded.match(/type:ANNUAL_REVENUE[^)]*rangeValue:\(min:(\d+(?:\.\d+)?)\)/);
  if (revMinOnly && result.revenue.min === null) {
    result.revenue.min = parseFloat(revMinOnly[1]);
  }

  // Dept headcount
  const deptHcRx = /type:DEPARTMENT_HEADCOUNT,selectedSubFilter:(\d+),values:List\(([^)]*(?:\([^)]*\)[^)]*)*)\)/g;
  let dm;
  while ((dm = deptHcRx.exec(decoded)) !== null) {
    extractValues(dm[2]).forEach(v => result.departmentHeadcount.push({
      dept: dm[1], id: v.id, label: v.text || HEADCOUNT_LABELS[v.id] || v.id,
    }));
  }

  // Dept growth
  const deptGrRx = /type:DEPARTMENT_HEADCOUNT_GROWTH,selectedSubFilter:(\d+),values:List\(([^)]*(?:\([^)]*\)[^)]*)*)\)/g;
  let dgm;
  while ((dgm = deptGrRx.exec(decoded)) !== null) {
    extractValues(dgm[2]).forEach(v => result.departmentGrowth.push({
      dept: dgm[1], id: v.id, label: v.text || HEADCOUNT_GROWTH_MAP[v.id] || v.id,
    }));
  }

  // Standard filters
  const fbRx = /\(type:([\w_]+),values:List\(([^()]*(?:\([^()]*\)[^()]*)*)\)(?:,selectionType:\w+)?\)/g;
  let fm;
  while ((fm = fbRx.exec(decoded)) !== null) {
    const fType = fm[1], vals = extractValues(fm[2]);
    switch (fType) {
      case 'COMPANY_HEADCOUNT':
        vals.forEach(v => result.headcounts.push({ id: v.id, label: v.text || HEADCOUNT_LABELS[v.id] || v.id, selectionType: v.selectionType }));
        break;
      case 'HEADCOUNT_GROWTH':
        vals.forEach(v => result.headcountGrowth.push({ id: v.id, label: v.text || HEADCOUNT_GROWTH_MAP[v.id] || v.id, selectionType: v.selectionType }));
        break;
      case 'INDUSTRY':
        vals.forEach(v => result.industries.push({ id: v.id, text: v.text || INDUSTRY_MAP[v.id] || `Industry ${v.id}`, selectionType: v.selectionType }));
        break;
      case 'REGION': case 'GEOGRAPHY':
        vals.forEach(v => result.regions.push({ id: v.id, text: v.text || v.id, selectionType: v.selectionType }));
        break;
      case 'COMPANY_TYPE':
        vals.forEach(v => result.companyTypes.push({ id: v.id, label: v.text || COMPANY_TYPE_MAP[v.id] || v.id, selectionType: v.selectionType }));
        break;
      case 'FORTUNE':     vals.forEach(v => result.fortune.push(v.text || FORTUNE_MAP[v.id] || `Fortune ${v.id}`)); break;
      case 'FOLLOWERS_OF': vals.forEach(v => result.followers.push({ id: v.id, label: v.text || FOLLOWERS_MAP[v.id] || v.id })); break;
      case 'TECHNOLOGIES_USED': vals.forEach(v => result.technologiesUsed.push({ id: v.id, text: v.text || v.id })); break;
      case 'SPOTLIGHT':   vals.forEach(v => result.spotlight.push({ id: v.id, text: v.text || v.id })); break;
      case 'CURRENT_COMPANY': vals.forEach(v => result.currentCompany.push({ id: v.id, text: v.text || v.id })); break;
      case 'PAST_COMPANY':    vals.forEach(v => result.pastCompany.push({ id: v.id, text: v.text || v.id })); break;
      case 'TITLE': case 'JOB_TITLE': vals.forEach(v => result.jobTitles.push(v.text || v.id)); break;
      case 'SENIORITY_LEVEL': vals.forEach(v => result.seniorityLevels.push(v.text || v.id)); break;
      case 'FUNCTION':        vals.forEach(v => result.functions.push(v.text || v.id)); break;
      case 'SCHOOL':          vals.forEach(v => result.schools.push(v.text || v.id)); break;
      case 'POSTAL_CODE':     vals.forEach(v => result.postalCodes.push(v.id)); break;
      default:
        if (!['recentSearchParam', 'spellCorrectionEnabled'].includes(fType))
          result.unknownFilters.push({ type: fType, values: vals });
    }
  }
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 2 ── VOYAGER API SEARCH  (replaces public HTML search)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * LinkedIn's Voyager API for company search.
 *
 * ENDPOINT:  GET https://www.linkedin.com/voyager/api/search/blended
 *
 * KEY PARAMS:
 *   keywords          – free text
 *   filters           – List((type:resultType,values:List((value:COMPANY))))
 *                       plus optional companySize, geoUrn, industry, etc.
 *   queryContext      – must include (headerType:SEARCH_BOX)
 *   origin            – SWITCH_SEARCH_VERTICAL
 *   start             – offset (0, 10, 20 …)
 *   count             – results per page (max 10 for free, 25 for some accounts)
 *
 * REQUIRED HEADERS:
 *   csrf-token / x-restli-protocol-version / x-li-lang / x-li-track
 *   (these specific values work without authentication for basic queries)
 *
 * The response shape:
 *   data.elements[].elements[].hitInfo.backendUrn  → "urn:li:company:12345"
 *   data.elements[].elements[].hitInfo.miniCompany.{name, universalName, ...}
 */
function buildVoyagerUrl(filters, start, count = 10) {
  const kw = (filters.keywords || '').trim() || 'company';

  // Build the filter list string for the Voyager API
  const filterParts = ['(type:resultType,values:List((value:COMPANY)))'];

  // Headcount sizes
  const sizes = [
    ...new Set(
      filters.headcounts
        .filter(h => h.selectionType !== 'EXCLUDED')
        .map(h => HEADCOUNT_TO_VOYAGER[h.id])
        .filter(Boolean)
    ),
  ];
  if (sizes.length > 0) {
    const vals = sizes.map(s => `(value:${s})`).join(',');
    filterParts.push(`(type:companySize,values:List(${vals}))`);
  }

  // Industries
  const industries = filters.industries
    .filter(i => i.selectionType !== 'EXCLUDED')
    .map(i => i.id);
  if (industries.length > 0) {
    const vals = industries.map(id => `(value:${id})`).join(',');
    filterParts.push(`(type:industryV2,values:List(${vals}))`);
  }

  // Regions → geoUrn
  const geoIds = filters.regions
    .filter(r => r.selectionType !== 'EXCLUDED')
    .map(r => r.id);
  if (geoIds.length > 0) {
    const vals = geoIds.map(id => `(value:urn%3Ali%3Ageo%3A${id})`).join(',');
    filterParts.push(`(type:geoUrn,values:List(${vals}))`);
  }

  // Company types
  const ctypes = filters.companyTypes
    .filter(c => c.selectionType !== 'EXCLUDED')
    .map(c => c.id);
  if (ctypes.length > 0) {
    const vals = ctypes.map(id => `(value:${id})`).join(',');
    filterParts.push(`(type:companyType,values:List(${vals}))`);
  }

  const filtersStr = `List(${filterParts.join(',')})`;

  const params = new URLSearchParams({
    keywords:     kw,
    filters:      filtersStr,
    queryContext: 'List((key:resultType,value:COMPANY))',
    origin:       'FACETED_SEARCH',
    start:        String(start),
    count:        String(count),
  });

  return `https://www.linkedin.com/voyager/api/search/blended?${params.toString()}`;
}

// ─────────────────────────────────────────────────────────────────────────────
//  HTTP UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

const sleep    = ms => new Promise(r => setTimeout(r, ms));
const randUA   = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const randInt  = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

/**
 * Headers for Voyager API calls.
 * csrf-token "ajax:0" is the standard unauthenticated token LinkedIn accepts.
 * Without the correct Csrf-Token + cookie pair you'll get 403 on authenticated
 * endpoints — but blended search works with this header set for basic queries.
 */
function makeVoyagerHeaders() {
  return {
    'User-Agent':                  randUA(),
    'Accept':                      'application/vnd.linkedin.normalized+json+2.1',
    'Accept-Language':             'en-US,en;q=0.9',
    'Accept-Encoding':             'gzip, deflate, br',
    'csrf-token':                  'ajax:0',
    'x-restli-protocol-version':   '2.0.0',
    'x-li-lang':                   'en_US',
    'x-li-track':                  '{"clientVersion":"1.13.9235","mpVersion":"1.13.9235","osName":"web","timezoneOffset":5.5,"timezone":"Asia/Calcutta","deviceFormFactor":"DESKTOP","mpName":"voyager-web"}',
    'x-li-page-instance':          'urn:li:page:d_flagship3_search_srp_companies;',
    'Referer':                     'https://www.linkedin.com/search/results/companies/',
    'Cookie':                      'JSESSIONID="ajax:0";',
    'Cache-Control':               'no-cache',
    'Sec-Fetch-Dest':              'empty',
    'Sec-Fetch-Mode':              'cors',
    'Sec-Fetch-Site':              'same-origin',
    'DNT':                         '1',
  };
}

function makeHtmlHeaders(referer = 'https://www.linkedin.com/') {
  return {
    'User-Agent':                randUA(),
    'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language':           'en-US,en;q=0.9',
    'Accept-Encoding':           'gzip, deflate, br',
    'Cache-Control':             'no-cache',
    'Pragma':                    'no-cache',
    'Referer':                   referer,
    'Sec-Fetch-Dest':            'document',
    'Sec-Fetch-Mode':            'navigate',
    'Sec-Fetch-Site':            referer.includes('linkedin') ? 'same-origin' : 'cross-site',
    'Upgrade-Insecure-Requests': '1',
    'DNT':                       '1',
  };
}

async function fetchJson(url, proxyUrl = null, retries = 4) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const opts = {
        url,
        headers:        makeVoyagerHeaders(),
        followRedirect: true,
        timeout:        { request: 40_000 },
        retry:          { limit: 0 },
        https:          { rejectUnauthorized: false },
        decompress:     true,
      };
      if (proxyUrl) opts.proxyUrl = proxyUrl;

      const res = await gotScraping(opts);

      if (res.statusCode === 200) {
        try {
          const json = JSON.parse(res.body);
          return { json, ok: true, status: 200 };
        } catch {
          console.warn(`    ⚠ Response was not valid JSON (status 200). Body snippet: ${res.body?.slice(0, 200)}`);
          return { json: null, ok: false, status: 200 };
        }
      }

      if (res.statusCode === 429 || res.statusCode === 999) {
        const wait = randInt(12000, 20000);
        console.warn(`    ⚠ Rate-limited (${res.statusCode}), waiting ${wait}ms…`);
        await sleep(wait);
        continue;
      }

      if (res.statusCode === 403) {
        // 403 on Voyager = needs real session. Fall through to HTML fallback.
        console.warn(`    ⚠ Voyager 403 — LinkedIn requires login for this query. Will try HTML fallback.`);
        return { json: null, ok: false, status: 403 };
      }

      console.warn(`    HTTP ${res.statusCode} → ${url.slice(0, 100)}`);
      return { json: null, ok: false, status: res.statusCode };

    } catch (e) {
      console.error(`    Attempt ${attempt}/${retries} error: ${e.message}`);
      if (attempt < retries) await sleep(randInt(3000, 7000));
    }
  }
  return { json: null, ok: false, status: 0 };
}

async function fetchHtml(url, proxyUrl = null, retries = 4) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const opts = {
        url,
        headers:        makeHtmlHeaders('https://www.linkedin.com/'),
        followRedirect: true,
        timeout:        { request: 40_000 },
        retry:          { limit: 0 },
        https:          { rejectUnauthorized: false },
        decompress:     true,
      };
      if (proxyUrl) opts.proxyUrl = proxyUrl;

      const res = await gotScraping(opts);
      if (res.statusCode === 200) return { html: res.body, ok: true, status: 200 };

      if (res.statusCode === 429 || res.statusCode === 999) {
        const wait = randInt(10000, 18000);
        console.warn(`    ⚠ Rate-limit (${res.statusCode}), waiting ${wait}ms…`);
        await sleep(wait);
        continue;
      }

      console.warn(`    HTTP ${res.statusCode} → ${url.slice(0, 100)}`);
      return { html: res.body, ok: false, status: res.statusCode };

    } catch (e) {
      console.error(`    Attempt ${attempt}/${retries} error: ${e.message}`);
      if (attempt < retries) await sleep(randInt(3000, 7000));
    }
  }
  return { html: null, ok: false, status: 0 };
}

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 3 ── PARSE VOYAGER JSON RESPONSE → COMPANY RECORDS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Extract company slugs + basic data directly from the Voyager JSON response.
 *
 * Voyager returns a nested structure. The relevant path is:
 *   json.data.elements[]           ← search result clusters
 *     .elements[]                  ← individual hits
 *       .hitInfo
 *         .backendUrn              → "urn:li:company:12345"
 *         .miniCompany
 *           .universalName         → slug (e.g. "google")
 *           .name                  → display name
 *           .industries            → [{localizedName}]
 *           .employeeCountRange    → {start, end}
 *           .headquarter           → {country, geographicArea, city}
 *           .followingInfo         → {followerCount}
 *
 * We also check the "included" array which sometimes carries richer objects.
 */
function parseVoyagerResponse(json) {
  const companies = [];
  if (!json) return companies;

  // Helper to dig miniCompany objects out of wherever LinkedIn put them
  const miniCompanies = new Map(); // universalName → miniCompany object

  // Check "included" array (LinkedIn normalised format)
  if (Array.isArray(json.included)) {
    for (const item of json.included) {
      if (item.$type === 'com.linkedin.voyager.organization.MiniCompany' && item.universalName) {
        miniCompanies.set(item.universalName, item);
      }
    }
  }

  // Walk data.elements
  const topElements = json?.data?.elements ?? json?.elements ?? [];
  for (const cluster of topElements) {
    const hits = cluster?.elements ?? [];
    for (const hit of hits) {
      const hi = hit?.hitInfo ?? hit;

      // Try to get the miniCompany block
      let mc = hi?.miniCompany ?? hi?.company ?? null;

      // Fallback: resolve via $recipeTypes / entityUrn link
      if (!mc && hi?.backendUrn) {
        const urnSlug = hi.backendUrn.replace('urn:li:company:', '');
        mc = miniCompanies.get(urnSlug) || null;
      }

      if (!mc?.universalName) continue;

      const slug = mc.universalName;
      const hq   = mc.headquarter ?? {};
      const emp  = mc.employeeCountRange ?? {};
      const inds = (mc.industries ?? []).map(i => i.localizedName || i.name || '').filter(Boolean);

      companies.push({
        slug,
        name:          mc.name                || null,
        industry:      inds.join(', ')        || null,
        employeeRange: emp.start != null ? `${emp.start}–${emp.end ?? ''}` : null,
        headquarters:  [hq.city, hq.geographicArea, hq.country].filter(Boolean).join(', ') || null,
        followerCount: mc.followingInfo?.followerCount ?? null,
        description:   mc.description         || null,
        logo:          mc.logoUrl             || mc.logo?.url || null,
        linkedinUrl:   `https://www.linkedin.com/company/${slug}/about/`,
        urn:           hi?.backendUrn         || null,
        _voyagerRaw:   mc,                    // keep raw for debugging
      });
    }
  }
  return companies;
}

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 4 ── HTML FALLBACK SEARCH (for when Voyager returns 403)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * If Voyager is fully blocked, fall back to scraping the HTML search page.
 *
 * LinkedIn's HTML search page still embeds some data in:
 *   1. <code> tags containing JSON (server-side rendered state)
 *   2. <a href="/company/..."> anchor tags
 *
 * We try to extract company slugs from both.
 */
function detectAuthWall(html) {
  return (
    html.includes('authwall') || html.includes('/login?') ||
    html.includes('Join to see') || html.includes('Sign in to view') ||
    html.includes('uas/authenticate') || html.includes('checkpoint/challenge')
  );
}

function extractFromHtmlFallback(html) {
  const $ = cheerio.load(html);
  const found = new Set();

  // 1. Try to parse embedded JSON state blobs in <code> tags
  $('code').each((_, el) => {
    try {
      const text = $(el).html() || '';
      if (!text.includes('universalName') && !text.includes('/company/')) return;
      const json = JSON.parse(text);
      // Walk all values looking for universalName strings
      const walk = (obj) => {
        if (!obj || typeof obj !== 'object') return;
        if (obj.universalName && typeof obj.universalName === 'string') {
          found.add(obj.universalName.toLowerCase());
        }
        for (const v of Object.values(obj)) walk(v);
      };
      walk(json);
    } catch { /* not parseable, skip */ }
  });

  // 2. Try <a> tags
  $('a[href*="/company/"]').each((_, el) => {
    const href = ($(el).attr('href') || '').split('?')[0].split('#')[0];
    const m = href.match(/\/company\/([a-zA-Z0-9_%-]+)/);
    if (!m) return;
    const slug = decodeURIComponent(m[1]).toLowerCase().replace(/\s+/g, '-');
    const SKIP = ['about','jobs','posts','people','insights','life','products','videos','events','mycompany'];
    if (!SKIP.includes(slug) && slug.length > 1) found.add(slug);
  });

  return {
    slugs:       [...found],
    isAuthWall:  detectAuthWall(html),
  };
}

function buildHtmlSearchUrl(filters, page) {
  const kw = (filters.keywords || '').trim() || 'company';
  const params = new URLSearchParams({
    keywords: kw,
    origin:   'FACETED_SEARCH',
    page:     String(page),
  });

  // Map headcount to public size codes
  const sizes = [...new Set(
    filters.headcounts
      .filter(h => h.selectionType !== 'EXCLUDED')
      .map(h => HEADCOUNT_TO_VOYAGER[h.id])
      .filter(Boolean)
  )];
  if (sizes.length > 0) params.set('companySize', sizes[0]); // public search only takes one

  // Industry
  if (filters.industries.length > 0) {
    const inc = filters.industries.filter(i => i.selectionType !== 'EXCLUDED');
    if (inc.length > 0) params.set('industry', inc[0].id);
  }

  // Geo URNs
  const geoIds = filters.regions.filter(r => r.selectionType !== 'EXCLUDED').map(r => r.id);
  if (geoIds.length > 0) {
    params.set('geoUrn', geoIds.map(id => `urn:li:geo:${id}`).join(','));
  }

  return `https://www.linkedin.com/search/results/companies/?${params.toString()}`;
}

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 5 ── SCRAPE INDIVIDUAL COMPANY /about/ PAGE  (same as v1)
// ─────────────────────────────────────────────────────────────────────────────

function parseJsonLd(html) {
  try {
    const $ = cheerio.load(html);
    let best = null;
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const obj = JSON.parse($(el).html() || '{}');
        if (obj?.['@type'] === 'Organization' || obj?.name) { best = obj; return false; }
      } catch { /* skip */ }
    });
    return best;
  } catch { return null; }
}

function pick($, ...selectors) {
  for (const sel of selectors) {
    const val = $(sel).first().text().trim();
    if (val) return val;
  }
  return null;
}

function scrapeCompanyAbout(html, sourceUrl) {
  const $ = cheerio.load(html);

  const name = pick($,
    'h1.top-card-layout__title', 'h1[class*="org-top-card"]',
    'h1[class*="artdeco"]', '.organization-outlet h1', 'h1',
  );
  const tagline      = pick($, 'h4.top-card-layout__second-subline', 'p.org-top-card-summary__tagline', '[data-test-id="about-us__tagline"]');
  const description  = pick($,
    'section.core-section-container p.core-section-container__main-description',
    'p[data-test-id="about-us__description"]', '[data-test-id="about-us"] p',
    '.org-about-us-organization-description__text', '.break-words p',
    '.core-section-container__main-description',
  );

  const stats = {};
  $('dl dt, dl dd').each((_, el) => {
    if (el.name === 'dt')       stats._k = $(el).text().trim().toLowerCase().replace(/[\s/]+/g, '_');
    else if (el.name === 'dd' && stats._k) { if (!stats[stats._k]) stats[stats._k] = $(el).text().trim(); stats._k = null; }
  });

  let website =
    $('a[data-test-id="about-us__website"]').attr('href') ||
    $('[data-tracking-control-name="about_website"] a').attr('href') ||
    stats['website'] || null;
  if (website?.includes('linkedin.com/redir')) {
    try { website = new URL(website).searchParams.get('url') || website; } catch { /* keep */ }
  }

  const hq          = pick($, '[data-test-id="about-us__headquarters"] dd', '[data-test-id="about-us__headquarters"]') || stats['headquarters'] || null;
  const industry    = pick($, '[data-test-id="about-us__industry"] dd', '[data-test-id="about-us__industry"]') || stats['industry'] || null;
  const companyType = pick($, '[data-test-id="about-us__organizationType"] dd', '[data-test-id="about-us__organizationType"]') || stats['type'] || null;
  const founded     = pick($, '[data-test-id="about-us__foundedOn"] dd', '[data-test-id="about-us__foundedOn"]') || stats['founded'] || null;
  const employeeCount = pick($,
    '[data-test-id="about-us__employeeCount"] dd', '[data-test-id="about-us__employeeCount"]',
    '.org-about-company-module__company-staff-count-range',
  ) || stats['company_size'] || null;
  const specialtiesRaw = pick($, '[data-test-id="about-us__specialties"] dd', '[data-test-id="about-us__specialties"]') || stats['specialties'] || '';
  const specialties = specialtiesRaw ? specialtiesRaw.split(',').map(s => s.trim()).filter(Boolean) : [];
  const followers   = pick($, 'span[data-test-id="followers-count"]', '.org-top-card-summary-info-list__info-item', '[data-test-id="followers-count"]');
  const logo        = $('img.org-top-card-primary-content__logo').attr('src') || $('img[data-ghost-classes*="logo"]').attr('src') || null;
  const canonicalUrl = $('link[rel="canonical"]').attr('href') || sourceUrl;

  return { name, tagline, description, website, hq, industry, companyType, founded, employeeCount, specialties, followers, logo, linkedinUrl: canonicalUrl };
}

// ─────────────────────────────────────────────────────────────────────────────
//  PROXY HELPER  (graceful free-tier fallback)
// ─────────────────────────────────────────────────────────────────────────────

async function getProxyUrl(useProxy, countryCode, tag = '') {
  if (!useProxy) return null;

  // Free tier only has datacenter; paid gets residential (much better for LinkedIn)
  const groupSets = [['RESIDENTIAL'], ['DATACENTER'], []];

  for (const groups of groupSets) {
    try {
      const label = groups.length ? groups[0] : 'AUTO';
      const cfg   = await Actor.createProxyConfiguration({
        groups: groups.length ? groups : undefined,
        countryCode,
      });
      const url = await cfg.newUrl(`li_${tag}_${Date.now()}`);
      if (tag === '') console.log(`    Using ${label} proxy (${countryCode})`);
      return url;
    } catch { /* try next tier */ }
  }

  console.warn('    ⚠ No proxy available — running without proxy (higher block risk)');
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
//  MAIN
// ─────────────────────────────────────────────────────────────────────────────

await Actor.init();

const input = (await Actor.getInput()) ?? {};
const {
  salesNavUrl      = '',
  keywordsOverride = null,
  maxCompanies     = 30,
  maxSearchPages   = 5,
  requestDelayMs   = 6000,
  deepScrape       = true,
  useApifyProxy    = true,
  proxyCountryCode = 'IN',
} = input;

if (!salesNavUrl.trim()) {
  console.error('❌  No salesNavUrl in input. Exiting.');
  await Actor.exit();
}

// ══════════════════════════════════════════════════════════════════════════════
//  STEP 1 — DECODE SALESNAV URL
// ══════════════════════════════════════════════════════════════════════════════

console.log('\n📋  STEP 1 — Decoding Sales Navigator URL…');
const filters = decodeSalesNavUrl(salesNavUrl);
if (keywordsOverride) filters.keywords = keywordsOverride;

console.log(`  Search type      : ${filters.searchType || 'unknown'}`);
console.log(`  Keywords         : ${filters.keywords || '(none)'}`);
console.log(`  Headcounts       : ${filters.headcounts.map(h => h.label).join(', ') || '(any)'}`);
console.log(`  Industries       : ${filters.industries.map(i => i.text).join(', ') || '(any)'}`);
console.log(`  Regions          : ${filters.regions.map(r => r.text).join(', ') || '(any)'}`);
console.log(`  Company types    : ${filters.companyTypes.map(c => c.label).join(', ') || '(any)'}`);
console.log(`  Revenue          : ${filters.revenue.min !== null ? `${filters.revenue.min}M–${filters.revenue.max ?? '∞'}M ${filters.revenue.currency}` : '(none)'}`);
console.log(`  Fortune          : ${filters.fortune.join(', ') || '(none)'}`);
console.log(`  Technologies     : ${filters.technologiesUsed.map(t => t.text).join(', ') || '(none)'}`);
if (filters.unknownFilters.length) console.log(`  Unknown filters  : ${filters.unknownFilters.map(f => f.type).join(', ')}`);

await Actor.setValue('DECODED_FILTERS', filters);

const metaOnlyFilters = [];
if (filters.revenue.min !== null)       metaOnlyFilters.push('ANNUAL_REVENUE');
if (filters.headcountGrowth.length)     metaOnlyFilters.push('HEADCOUNT_GROWTH');
if (filters.fortune.length)             metaOnlyFilters.push('FORTUNE');
if (filters.followers.length)           metaOnlyFilters.push('FOLLOWERS_OF');
if (filters.technologiesUsed.length)    metaOnlyFilters.push('TECHNOLOGIES_USED');
if (filters.spotlight.length)           metaOnlyFilters.push('SPOTLIGHT');
if (filters.departmentHeadcount.length) metaOnlyFilters.push('DEPARTMENT_HEADCOUNT');
if (filters.departmentGrowth.length)    metaOnlyFilters.push('DEPARTMENT_HEADCOUNT_GROWTH');

if (metaOnlyFilters.length) {
  console.log(`\n  ⚠  Decoded but cannot apply to search: ${metaOnlyFilters.join(', ')}`);
  console.log(`     Saved to output metadata for reference.`);
}

// ══════════════════════════════════════════════════════════════════════════════
//  STEP 2+3 — SEARCH VIA VOYAGER API (with HTML fallback)
// ══════════════════════════════════════════════════════════════════════════════

console.log('\n🔍  STEP 2 — Searching via Voyager API…');

/**
 * We attempt Voyager first. If it consistently returns 403 or empty results
 * (which happens when LinkedIn detects scraping without a real session), we
 * automatically fall back to HTML search page scraping.
 */

const companyMap = new Map(); // slug → basic data object
let voyagerFailed403Count = 0;
let useHtmlFallback = false;

// Get one proxy URL to reuse for the search phase
const searchProxy = await getProxyUrl(useApifyProxy, proxyCountryCode, 'search');

// ── Voyager API pages ──────────────────────────────────────────────────────
for (let page = 0; page < maxSearchPages && companyMap.size < maxCompanies; page++) {
  const start = page * 10;
  const apiUrl = buildVoyagerUrl(filters, start, 10);

  console.log(`\n  [Page ${page + 1}/${maxSearchPages}] Voyager API start=${start}`);
  console.log(`    ${apiUrl.slice(0, 120)}`);

  const { json, ok, status } = await fetchJson(apiUrl, searchProxy);

  if (status === 403) {
    voyagerFailed403Count++;
    if (voyagerFailed403Count >= 2) {
      console.warn('\n  ⚠ Voyager returning 403 consistently — switching to HTML fallback.');
      useHtmlFallback = true;
      break;
    }
    await sleep(randInt(requestDelayMs, requestDelayMs + 3000));
    continue;
  }

  if (!ok || !json) {
    console.warn(`    ⚠ No data (status ${status}), skipping page.`);
    await sleep(randInt(requestDelayMs, requestDelayMs + 3000));
    continue;
  }

  const companies = parseVoyagerResponse(json);
  console.log(`    ✅ Got ${companies.length} company record(s) from Voyager`);

  if (companies.length === 0) {
    // Save debug snapshot for first empty page
    if (page === 0) {
      await Actor.setValue('debug_voyager_page0_json', json);
      console.warn('    Saved empty Voyager response to KV store as debug_voyager_page0_json');
    }
    // If first two pages are empty, also try HTML fallback
    if (page >= 1) {
      console.warn('    Two consecutive empty pages — switching to HTML fallback.');
      useHtmlFallback = true;
      break;
    }
  }

  for (const co of companies) {
    if (companyMap.size >= maxCompanies) break;
    companyMap.set(co.slug, co);
  }

  await sleep(randInt(requestDelayMs, requestDelayMs + 2000));
}

// ── HTML fallback ──────────────────────────────────────────────────────────
if (useHtmlFallback || companyMap.size === 0) {
  console.log('\n  📄  Attempting HTML search fallback…');

  for (let page = 1; page <= maxSearchPages && companyMap.size < maxCompanies; page++) {
    const htmlUrl = buildHtmlSearchUrl(filters, page);
    console.log(`\n  [HTML Page ${page}] ${htmlUrl.slice(0, 120)}`);

    const { html, ok, status } = await fetchHtml(htmlUrl, searchProxy);

    if (!html || !ok) {
      console.warn(`    ⚠ Skipped (HTTP ${status})`);
      await sleep(randInt(requestDelayMs, requestDelayMs + 2000));
      continue;
    }

    const { slugs, isAuthWall } = extractFromHtmlFallback(html);

    if (isAuthWall) {
      console.warn(`    ⚠ Auth wall — LinkedIn blocking unauthenticated access.`);
      console.warn(`      → Enable Apify Proxy with RESIDENTIAL group (paid plan needed).`);
      await Actor.setValue(`debug_html_search_page${page}`, html, { contentType: 'text/html' });
      await sleep(randInt(requestDelayMs * 2, requestDelayMs * 3));
      continue;
    }

    console.log(`    ✅ Found ${slugs.length} company slug(s) in HTML`);
    for (const slug of slugs) {
      if (companyMap.size >= maxCompanies) break;
      if (!companyMap.has(slug)) {
        companyMap.set(slug, { slug, linkedinUrl: `https://www.linkedin.com/company/${slug}/about/` });
      }
    }

    await sleep(randInt(requestDelayMs, requestDelayMs + 2000));
  }
}

console.log(`\n📦  Total unique companies queued: ${companyMap.size}`);

if (companyMap.size === 0) {
  console.error('\n❌  No companies found. Diagnosis:');
  console.error('    • Voyager API: LinkedIn 403 = requires authenticated session.');
  console.error('    • HTML fallback: Auth wall = IP is blocked by LinkedIn.');
  console.error('    SOLUTIONS:');
  console.error('    1. Upgrade to Apify paid plan → use RESIDENTIAL proxies (most effective).');
  console.error('    2. Increase requestDelayMs to 10000+ ms.');
  console.error('    3. Verify your proxyCountryCode matches the search region (e.g. IN for India).');
  console.error('    4. Check debug_voyager_page0_json in Key-Value Store for raw API response.');
  await Actor.exit();
}

// Build the filter context summary for output records
const filterContext = {
  keywords:         filters.keywords,
  headcounts:       filters.headcounts.map(h => h.label),
  headcountGrowth:  filters.headcountGrowth.map(h => h.label),
  industries:       filters.industries.map(i => i.text),
  regions:          filters.regions.map(r => r.text),
  companyTypes:     filters.companyTypes.map(c => c.label),
  revenue:          filters.revenue.min !== null ? `${filters.revenue.min}M–${filters.revenue.max ?? '∞'}M ${filters.revenue.currency}` : null,
  fortune:          filters.fortune,
  technologiesUsed: filters.technologiesUsed.map(t => t.text),
  spotlight:        filters.spotlight.map(s => s.text),
  metaOnlyFilters,
};

// ══════════════════════════════════════════════════════════════════════════════
//  STEP 4 — DEEP SCRAPE /about/ PAGES  (optional)
// ══════════════════════════════════════════════════════════════════════════════

const companyList = [...companyMap.values()];
console.log(`\n🏢  STEP 3 — ${deepScrape ? 'Deep-scraping' : 'Outputting (shallow)'} ${companyList.length} companies…`);

for (let ci = 0; ci < companyList.length; ci++) {
  const base = companyList[ci];
  const { slug } = base;

  console.log(`\n  [${ci + 1}/${companyList.length}] ${slug}`);

  if (!deepScrape) {
    await Actor.pushData({
      ...base,
      scrapeMode:    'shallow',
      filterContext,
      scrapedAt:     new Date().toISOString(),
      _voyagerRaw:   undefined, // strip debug field from output
    });
    continue;
  }

  const aboutUrl   = `https://www.linkedin.com/company/${slug}/about/`;
  const aboutProxy = await getProxyUrl(useApifyProxy, proxyCountryCode, slug.slice(0, 8));
  const { html, ok, status } = await fetchHtml(aboutUrl, aboutProxy);

  if (!html) {
    console.error(`    ❌ Fetch failed for ${slug}`);
    await Actor.pushData({
      ...base, error: 'Fetch failed', httpStatus: status,
      scrapeMode: 'deep_failed', filterContext, scrapedAt: new Date().toISOString(),
    });
    continue;
  }

  if (detectAuthWall(html)) {
    console.warn(`    ⚠ Auth wall for ${slug}`);
    await Actor.pushData({
      ...base, error: 'Auth wall on /about/ page', httpStatus: status,
      scrapeMode: 'deep_blocked', filterContext, scrapedAt: new Date().toISOString(),
    });
    await sleep(randInt(requestDelayMs * 2, requestDelayMs * 3));
    continue;
  }

  const jsonLd = parseJsonLd(html);
  const page   = scrapeCompanyAbout(html, aboutUrl);

  // Merge Voyager basic data + JSON-LD + Cheerio-scraped fields
  const record = {
    // Identity (prefer jsonLd > voyager > page)
    name:           jsonLd?.name           || base.name  || page.name,
    tagline:        page.tagline,
    description:    jsonLd?.description    || base.description || page.description,

    // Contact
    website:        jsonLd?.url            || page.website,
    email:          jsonLd?.email          || null,
    phone:          jsonLd?.telephone      || null,

    // Location
    headquarters:   page.hq               || base.headquarters || jsonLd?.address?.addressLocality || null,
    streetAddress:  jsonLd?.address?.streetAddress  || null,
    city:           jsonLd?.address?.addressLocality || null,
    stateRegion:    jsonLd?.address?.addressRegion   || null,
    country:        jsonLd?.address?.addressCountry  || null,
    postalCode:     jsonLd?.address?.postalCode       || null,
    fullAddress:    jsonLd?.address
      ? [jsonLd.address.streetAddress, jsonLd.address.addressLocality,
         jsonLd.address.addressRegion, jsonLd.address.postalCode,
         jsonLd.address.addressCountry].filter(Boolean).join(', ')
      : null,

    // Company details
    industry:       page.industry         || base.industry,
    companyType:    page.companyType,
    founded:        page.founded          || jsonLd?.foundingDate || null,
    employeeCount:  page.employeeCount    || base.employeeRange,
    specialties:    page.specialties,

    // Social
    followers:      page.followers        || (base.followerCount ? String(base.followerCount) : null),
    logo:           page.logo             || base.logo || jsonLd?.logo || null,

    // Links
    linkedinUrl:    page.linkedinUrl      || aboutUrl,
    sameAs:         jsonLd?.sameAs        || [],

    // Context
    filterContext,
    revenueFilterApplied: filters.revenue.min !== null
      ? { min: filters.revenue.min, max: filters.revenue.max, currency: filters.revenue.currency,
          note: 'Decoded from SalesNav URL — the search filter range, NOT confirmed company revenue' }
      : null,
    headcountGrowthFilterApplied: filters.headcountGrowth.length
      ? filters.headcountGrowth.map(h => h.label) : null,

    // Meta
    slug,
    httpStatus:  status,
    scrapeMode:  'deep',
    scrapedAt:   new Date().toISOString(),
  };

  const label = `${record.name || '(no name)'} | ${record.industry || 'n/a'} | ${record.employeeCount || 'n/a'}`;
  console.log(`    ✅ ${label}`);

  await Actor.pushData(record);

  if (ci < companyList.length - 1) await sleep(randInt(requestDelayMs, requestDelayMs + 2500));
}

// ══════════════════════════════════════════════════════════════════════════════
//  DONE
// ══════════════════════════════════════════════════════════════════════════════

const ds = await Actor.openDataset();
const { itemCount } = await ds.getInfo();
console.log(`\n🏁  Done! ${itemCount} company records saved to Dataset.`);
console.log('    Export as JSON / CSV / Excel from Apify Console → Storage → Datasets.');

await Actor.exit();
