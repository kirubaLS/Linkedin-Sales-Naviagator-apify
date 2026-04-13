/**
 * ═══════════════════════════════════════════════════════════════════════════
 *  LinkedIn SalesNav → Company Scraper  |  Apify Actor  v3
 *  Node 18 · Educational use only
 * ═══════════════════════════════════════════════════════════════════════════
 *
 *  v3 ARCHITECTURE — why v1/v2 failed and what we do instead:
 *  ─────────────────────────────────────────────────────────────
 *  ✗ /search/results/companies/ HTML  → 100% React shell, no data
 *  ✗ /voyager/api/search/blended      → 404, endpoint deprecated
 *  ✗ /voyager/api/graphql             → needs real CSRF + li_at cookie
 *
 *  ✓ STRATEGY A (primary):
 *    LinkedIn's public typeahead endpoint (powers the search-as-you-type bar):
 *      GET /voyager/api/typeahead/hitsV2?keywords=...&type=COMPANY
 *    This does NOT require authentication or CSRF. Returns JSON with
 *    company name, slug, industry, headcount, HQ, logo, followerCount.
 *    We call it with multiple keyword variants to maximise coverage.
 *
 *  ✓ STRATEGY B (secondary):
 *    LinkedIn public search HTML with aggressive JSON extraction.
 *    The page is React-rendered but embeds state in <script> and <code> tags.
 *    We scan all inline JSON for "universalName" keys and raw href patterns.
 *
 *  ✓ STRATEGY C (tertiary):
 *    Google/Bing SERP scraping — site:linkedin.com/company "keywords" "city"
 *    Highly effective because Google has indexed all public LinkedIn pages.
 *    No auth required. Works with any proxy.
 *
 *  ✓ STRATEGY D (deep data):
 *    /company/<slug>/about/ pages are SERVER-SIDE RENDERED and contain
 *    rich JSON-LD Organization schema. Always works if IP isn't blocked.
 * ═══════════════════════════════════════════════════════════════════════════
 */

import { Actor } from 'apify';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

// ─────────────────────────────────────────────────────────────────────────────
//  LOOKUP TABLES
// ─────────────────────────────────────────────────────────────────────────────

const HEADCOUNT_LABELS = {
  A: '1-10', B: '11-50', C: '51-200', D: '51-200',
  E: '201-500', F: '501-1000', G: '1001-5000', H: '5001-10000', I: '10001+',
};

const HEADCOUNT_TO_SIZE = {
  A: 'A', B: 'B', C: 'C', D: 'C', E: 'D', F: 'E', G: 'F', H: 'G', I: 'H',
};

const INDUSTRY_MAP = {
  '1': 'Defense and Space Manufacturing', '3': 'Computer Hardware Manufacturing',
  '4': 'Software Development', '5': 'Computer Networking Products',
  '6': 'Technology, Information and Internet', '7': 'Telecommunications',
  '8': 'Semiconductor Manufacturing', '9': 'Computers and Electronics Manufacturing',
  '10': 'Transportation Equipment Manufacturing', '11': 'Higher Education',
  '12': 'Primary and Secondary Education', '13': 'Education Administration Programs',
  '14': 'Research Services', '23': 'Management Consulting',
  '24': 'IT Services and IT Consulting', '28': 'Construction',
  '30': 'Real Estate', '41': 'Medical Practices',
  '42': 'Hospitals and Health Care', '43': 'Pharmaceutical Manufacturing',
  '44': 'Biotechnology Research', '46': 'Medical Equipment Manufacturing',
  '52': 'Aviation and Aerospace Component Manufacturing',
  '60': 'Chemicals Manufacturing', '63': 'Entertainment Providers',
  '71': 'Advertising Services', '72': 'Marketing Services',
  '82': 'Oil and Gas', '86': 'Strategic Management Services',
  '87': 'Business Consulting and Services', '88': 'Human Resources Services',
  '89': 'Staffing and Recruiting', '91': 'Financial Services',
  '92': 'Investment Banking', '93': 'Investment Management',
  '94': 'Accounting', '95': 'Insurance', '96': 'Banking',
  '97': 'Venture Capital and Private Equity Principals',
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
  '1': 'USD', '2': 'EUR', '3': 'GBP', '4': 'INR', '5': 'CAD',
  '6': 'AUD', '7': 'CNY', '8': 'JPY', '9': 'BRL', '10': 'MXN',
};

const COMPANY_TYPE_MAP = {
  'C': 'Public Company', 'D': 'Privately Held', 'E': 'Non-profit',
  'B': 'Partnership', 'A': 'Self-Employed', 'F': 'Government Agency',
  'G': 'Educational Institution', 'H': 'Sole Proprietorship',
};

const HEADCOUNT_GROWTH_MAP = {
  'ABOVE_40': 'Above 40% growth', 'BETWEEN_20_AND_40': '20%-40% growth',
  'BETWEEN_10_AND_20': '10%-20% growth', 'BETWEEN_5_AND_10': '5%-10% growth',
  'BETWEEN_0_AND_5': '0%-5% growth', 'NEGATIVE': 'Negative growth',
};

const FORTUNE_MAP = {
  '10': 'Fortune 10', '50': 'Fortune 50', '100': 'Fortune 100', '500': 'Fortune 500',
};

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
];

// ─────────────────────────────────────────────────────────────────────────────
//  SALESNAV URL DECODER
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
    keywords: null, searchType: null,
    headcounts: [], headcountGrowth: [], industries: [], regions: [],
    companyTypes: [], revenue: { min: null, max: null, currency: 'USD' },
    fortune: [], followers: [], technologiesUsed: [], departmentHeadcount: [],
    departmentGrowth: [], spotlight: [], currentCompany: [], pastCompany: [],
    postalCodes: [], unknownFilters: [], rawDecoded: decoded,
  };

  if (decoded.includes('/sales/search/company'))     result.searchType = 'company';
  else if (decoded.includes('/sales/search/people')) result.searchType = 'people';

  const kwM = decoded.match(/[,(?]keywords:([^,)&\n]+)/);
  if (kwM) result.keywords = safeDecodeUri(kwM[1].trim());

  const revM = decoded.match(
    /type:ANNUAL_REVENUE[^)]*rangeValue:\(min:(\d+(?:\.\d+)?),max:(\d+(?:\.\d+)?)\)(?:[^)]*selectedSubFilter:(\d+))?/
  );
  if (revM) {
    result.revenue.min      = parseFloat(revM[1]);
    result.revenue.max      = parseFloat(revM[2]);
    result.revenue.currency = REVENUE_CURRENCY_MAP[revM[3]] || 'USD';
  }

  const deptHcRx = /type:DEPARTMENT_HEADCOUNT,selectedSubFilter:(\d+),values:List\(([^)]*(?:\([^)]*\)[^)]*)*)\)/g;
  let dm;
  while ((dm = deptHcRx.exec(decoded)) !== null) {
    extractValues(dm[2]).forEach(v => result.departmentHeadcount.push({
      dept: dm[1], id: v.id, label: v.text || HEADCOUNT_LABELS[v.id] || v.id,
    }));
  }

  const fbRx = /\(type:([\w_]+),values:List\(([^()]*(?:\([^()]*\)[^()]*)*)\)(?:,selectionType:\w+)?\)/g;
  let fm;
  while ((fm = fbRx.exec(decoded)) !== null) {
    const fType = fm[1], vals = extractValues(fm[2]);
    switch (fType) {
      case 'COMPANY_HEADCOUNT':
        vals.forEach(v => result.headcounts.push({ id: v.id, label: v.text || HEADCOUNT_LABELS[v.id] || v.id, selectionType: v.selectionType })); break;
      case 'HEADCOUNT_GROWTH':
        vals.forEach(v => result.headcountGrowth.push({ id: v.id, label: v.text || HEADCOUNT_GROWTH_MAP[v.id] || v.id, selectionType: v.selectionType })); break;
      case 'INDUSTRY':
        vals.forEach(v => result.industries.push({ id: v.id, text: v.text || INDUSTRY_MAP[v.id] || `Industry ${v.id}`, selectionType: v.selectionType })); break;
      case 'REGION': case 'GEOGRAPHY':
        vals.forEach(v => result.regions.push({ id: v.id, text: v.text || v.id, selectionType: v.selectionType })); break;
      case 'COMPANY_TYPE':
        vals.forEach(v => result.companyTypes.push({ id: v.id, label: v.text || COMPANY_TYPE_MAP[v.id] || v.id, selectionType: v.selectionType })); break;
      case 'FORTUNE':
        vals.forEach(v => result.fortune.push(v.text || FORTUNE_MAP[v.id] || `Fortune ${v.id}`)); break;
      case 'FOLLOWERS_OF':
        vals.forEach(v => result.followers.push({ id: v.id, label: v.text || v.id })); break;
      case 'TECHNOLOGIES_USED':
        vals.forEach(v => result.technologiesUsed.push({ id: v.id, text: v.text || v.id })); break;
      case 'SPOTLIGHT':
        vals.forEach(v => result.spotlight.push({ id: v.id, text: v.text || v.id })); break;
      case 'CURRENT_COMPANY':
        vals.forEach(v => result.currentCompany.push({ id: v.id, text: v.text || v.id })); break;
      case 'PAST_COMPANY':
        vals.forEach(v => result.pastCompany.push({ id: v.id, text: v.text || v.id })); break;
      case 'POSTAL_CODE':
        vals.forEach(v => result.postalCodes.push(v.id)); break;
      default:
        if (!['recentSearchParam', 'spellCorrectionEnabled'].includes(fType))
          result.unknownFilters.push({ type: fType, values: vals });
    }
  }
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
//  HTTP HELPER
// ─────────────────────────────────────────────────────────────────────────────

const sleep   = ms => new Promise(r => setTimeout(r, ms));
const randUA  = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const randInt = (lo, hi) => Math.floor(Math.random() * (hi - lo + 1)) + lo;

function htmlHeaders(referer = 'https://www.google.com/') {
  return {
    'User-Agent':                randUA(),
    'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language':           'en-US,en;q=0.9',
    'Accept-Encoding':           'gzip, deflate, br',
    'Cache-Control':             'no-cache',
    'Pragma':                    'no-cache',
    'Sec-Fetch-Dest':            'document',
    'Sec-Fetch-Mode':            'navigate',
    'Sec-Fetch-Site':            referer.includes('linkedin') ? 'same-origin' : 'cross-site',
    'Sec-Fetch-User':            '?1',
    'Upgrade-Insecure-Requests': '1',
    'Referer':                   referer,
    'DNT':                       '1',
  };
}

// Headers for the typeahead endpoint — NO csrf-token needed
function typeaheadHeaders() {
  return {
    'User-Agent':                randUA(),
    'Accept':                    'application/vnd.linkedin.normalized+json+2.1',
    'Accept-Language':           'en-US,en;q=0.9',
    'Accept-Encoding':           'gzip, deflate, br',
    'x-restli-protocol-version': '2.0.0',
    'x-li-lang':                 'en_US',
    'x-li-track':                JSON.stringify({
      clientVersion: '1.13.10438', mpVersion: '1.13.10438',
      osName: 'web', timezoneOffset: 5.5, timezone: 'Asia/Calcutta',
      deviceFormFactor: 'DESKTOP', mpName: 'voyager-web',
    }),
    'Referer':                   'https://www.linkedin.com/',
    'Sec-Fetch-Dest':            'empty',
    'Sec-Fetch-Mode':            'cors',
    'Sec-Fetch-Site':            'same-origin',
    'DNT':                       '1',
  };
}

async function get(url, headers, proxyUrl, retries = 4) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const opts = {
        url, headers,
        followRedirect: true,
        timeout:        { request: 45_000 },
        retry:          { limit: 0 },
        https:          { rejectUnauthorized: false },
        decompress:     true,
      };
      if (proxyUrl) opts.proxyUrl = proxyUrl;

      const res = await gotScraping(opts);

      if (res.statusCode === 200) return { body: res.body, ok: true, status: 200 };

      if (res.statusCode === 429 || res.statusCode === 999) {
        const wait = randInt(15000, 25000);
        console.warn(`    ⚠ Rate-limited (${res.statusCode}) — waiting ${wait}ms`);
        await sleep(wait);
        continue;
      }

      console.warn(`    HTTP ${res.statusCode} for ${url.slice(0, 90)}`);
      return { body: res.body || null, ok: false, status: res.statusCode };

    } catch (err) {
      console.error(`    Attempt ${attempt}/${retries}: ${err.message}`);
      if (attempt < retries) await sleep(randInt(4000, 9000));
    }
  }
  return { body: null, ok: false, status: 0 };
}

// ─────────────────────────────────────────────────────────────────────────────
//  STRATEGY A — LinkedIn Typeahead  (no auth needed)
// ─────────────────────────────────────────────────────────────────────────────

function buildTypeaheadUrl(keywords, count = 10) {
  const params = new URLSearchParams({
    keywords,
    type:    'COMPANY',
    origin:  'OTHER',
    useCase: 'PEOPLE_SEARCH',
    count:   String(count),
  });
  return `https://www.linkedin.com/voyager/api/typeahead/hitsV2?${params}`;
}

function parseTypeaheadResponse(body) {
  const companies = [];
  let json;
  try { json = JSON.parse(body); } catch { return companies; }

  const elements = json?.data?.elements ?? json?.elements ?? [];
  for (const el of elements) {
    const mc = el?.hitInfo?.miniCompany ?? el?.company ?? el?.miniCompany ?? null;
    if (!mc?.universalName) continue;

    const hq  = mc.headquarter ?? {};
    const emp = mc.employeeCountRange ?? {};
    const ind = (mc.industries ?? []).map(i => i.localizedName || i.name || '').filter(Boolean);

    companies.push({
      slug:          mc.universalName,
      name:          mc.name || null,
      industry:      ind.join(', ') || null,
      employeeRange: emp.start != null ? `${emp.start}–${emp.end ?? '+'}` : null,
      headquarters:  [hq.city, hq.geographicArea, hq.country].filter(Boolean).join(', ') || null,
      followerCount: mc.followingInfo?.followerCount ?? null,
      description:   mc.description || null,
      logo:          mc.logoUrl || mc.logo?.url || null,
      linkedinUrl:   `https://www.linkedin.com/company/${mc.universalName}/about/`,
    });
  }
  return companies;
}

// Build keyword variants to maximise typeahead coverage
function generateKeywordVariants(filters) {
  const baseKw = (filters.keywords || '').trim();
  const variants = new Set();

  if (!baseKw) { variants.add('technology'); return [...variants]; }

  variants.add(baseKw);

  // Region-qualified variants
  for (const region of filters.regions.slice(0, 2)) {
    if (!region.text || region.text.match(/^\d+$/)) continue;
    const city = region.text
      .replace(/Greater\s+/i, '')
      .replace(/\s+Area$/i, '')
      .replace(/\s+Metropolitan.*/i, '')
      .trim();
    if (city) {
      variants.add(`${baseKw} ${city}`);
      const lc = city.toLowerCase();
      if (lc.includes('bengaluru') || lc.includes('bangalore')) {
        variants.add(`${baseKw} bangalore`);
        variants.add(`${baseKw} india`);
        variants.add(`${baseKw} Bengaluru`);
      }
      if (lc.includes('mumbai'))   variants.add(`${baseKw} bombay`);
      if (lc.includes('chennai'))  variants.add(`${baseKw} madras`);
      if (lc.includes('kolkata'))  variants.add(`${baseKw} calcutta`);
      if (lc.includes('hyderabad')) variants.add(`${baseKw} hyderabad`);
    }
  }

  // Industry-qualified variants
  for (const ind of filters.industries.slice(0, 2)) {
    if (ind.text && ind.text.length < 40) variants.add(`${ind.text}`);
  }

  // Common synonym expansions
  const lkw = baseKw.toLowerCase();
  const synonymMap = {
    'machine learning': ['ML company', 'deep learning', 'AI machine learning', 'data science'],
    'artificial intelligence': ['AI company', 'machine learning'],
    'software': ['software development', 'SaaS platform'],
    'fintech': ['financial technology', 'payments startup'],
    'healthtech': ['health technology', 'medtech'],
    'edtech': ['education technology', 'e-learning'],
  };
  for (const [key, syns] of Object.entries(synonymMap)) {
    if (lkw.includes(key)) syns.forEach(s => variants.add(s));
  }

  return [...variants].slice(0, 10);
}

// ─────────────────────────────────────────────────────────────────────────────
//  STRATEGY B — LinkedIn HTML Search with JSON extraction
// ─────────────────────────────────────────────────────────────────────────────

function buildPublicSearchUrl(filters, page) {
  const kw = (filters.keywords || '').trim() || 'technology';
  const params = new URLSearchParams({
    keywords: kw,
    origin:   'FACETED_SEARCH',
    page:     String(page),
  });

  const sizes = [...new Set(
    filters.headcounts
      .filter(h => h.selectionType !== 'EXCLUDED')
      .map(h => HEADCOUNT_TO_SIZE[h.id])
      .filter(Boolean)
  )];
  if (sizes.length > 0) params.set('companySize', sizes[0]);

  const incInds = filters.industries.filter(i => i.selectionType !== 'EXCLUDED');
  if (incInds.length > 0) params.set('industry', incInds[0].id);

  const geoIds = filters.regions.filter(r => r.selectionType !== 'EXCLUDED').map(r => r.id);
  if (geoIds.length > 0) {
    // Must be raw URN string; URLSearchParams will encode it
    params.set('geoUrn', geoIds.map(id => `urn:li:geo:${id}`).join(','));
  }

  return `https://www.linkedin.com/search/results/companies/?${params}`;
}

const SLUG_SKIP = new Set([
  'about','jobs','posts','people','insights','life','products','videos',
  'events','mycompany','pulse','showcase','search','directory','school',
  'groups','linkedin','company','results',
]);

function extractSlugsFromHtml(html) {
  const slugs = new Set();

  // 1. Raw regex on whole HTML — catches slugs in JS bundles, data attrs, etc.
  for (const m of html.matchAll(/\/company\/([a-zA-Z0-9_-]{2,60})\//g)) {
    const s = m[1].toLowerCase();
    if (!SLUG_SKIP.has(s)) slugs.add(s);
  }

  // 2. "universalName":"value" patterns in embedded JSON
  for (const m of html.matchAll(/"universalName"\s*:\s*"([a-zA-Z0-9_-]{2,60})"/g)) {
    const s = m[1].toLowerCase();
    if (!SLUG_SKIP.has(s) && s.length > 2) slugs.add(s);
  }

  // 3. <code> tag JSON blobs (LinkedIn SSR state)
  const $ = cheerio.load(html);
  $('code').each((_, el) => {
    const content = $(el).html() || '';
    if (!content.includes('universalName')) return;
    try {
      const walk = (obj) => {
        if (!obj || typeof obj !== 'object') return;
        if (typeof obj.universalName === 'string' && obj.universalName.length > 2) {
          const s = obj.universalName.toLowerCase();
          if (!SLUG_SKIP.has(s)) slugs.add(s);
        }
        for (const v of Object.values(obj)) walk(v);
      };
      walk(JSON.parse(content));
    } catch { /* not JSON */ }
  });

  const isAuthWall =
    html.includes('authwall') || html.includes('/login?') ||
    html.includes('uas/authenticate') || html.includes('checkpoint/challenge');

  return { slugs: [...slugs], isAuthWall };
}

// ─────────────────────────────────────────────────────────────────────────────
//  STRATEGY C — Google / Bing SERP
// ─────────────────────────────────────────────────────────────────────────────

function buildGoogleQuery(filters) {
  const kw = (filters.keywords || 'technology').trim();
  const parts = [`site:linkedin.com/company "${kw}"`];

  for (const region of filters.regions.slice(0, 1)) {
    if (region.text && !region.text.match(/^\d+$/)) {
      const city = region.text.replace(/Greater\s+/i, '').replace(/\s+Area$/i, '').trim();
      if (city) parts.push(`"${city}"`);
    }
  }

  return parts.join(' ');
}

function buildGoogleUrl(query, page = 1) {
  const params = new URLSearchParams({
    q: query, num: '10', hl: 'en', gl: 'in', start: String((page - 1) * 10),
  });
  return `https://www.google.com/search?${params}`;
}

function buildBingUrl(query, page = 1) {
  const params = new URLSearchParams({
    q: query, count: '10', mkt: 'en-IN', first: String((page - 1) * 10 + 1),
  });
  return `https://www.bing.com/search?${params}`;
}

function extractSlugsFromSerp(html) {
  const slugs = new Set();
  for (const m of html.matchAll(/linkedin\.com\/company\/([a-zA-Z0-9_-]{2,60})/g)) {
    const s = m[1].toLowerCase().split('/')[0];
    if (!SLUG_SKIP.has(s) && s.length > 2) slugs.add(s);
  }
  return [...slugs];
}

// ─────────────────────────────────────────────────────────────────────────────
//  COMPANY /about/ SCRAPER
// ─────────────────────────────────────────────────────────────────────────────

function detectAuthWall(html) {
  if (!html) return false;
  return (
    html.includes('authwall') || html.includes('/login?') ||
    html.includes('uas/authenticate') || html.includes('checkpoint/challenge') ||
    (html.includes('Sign in') && html.includes('Join now') && !html.includes('data-test-id'))
  );
}

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

  const ogTitle = $('meta[property="og:title"]').attr('content')?.split('|')[0]?.trim() || null;
  const ogDesc  = $('meta[property="og:description"]').attr('content') || null;
  const ogImage = $('meta[property="og:image"]').attr('content') || null;

  const name = pick($,
    'h1.top-card-layout__title', 'h1[class*="org-top-card"]',
    'h1[class*="artdeco-entity-lockup__title"]', 'h1',
  ) || ogTitle;

  const description = pick($,
    'p[data-test-id="about-us__description"]', '[data-test-id="about-us"] p',
    'section.core-section-container p.core-section-container__main-description',
    '.org-about-us-organization-description__text', '.break-words p',
    '.core-section-container__main-description',
  ) || ogDesc;

  const tagline = pick($,
    'h4.top-card-layout__second-subline', 'p.org-top-card-summary__tagline',
    '[data-test-id="about-us__tagline"]',
  );

  // dt/dd pairs
  const stats = {};
  $('dl dt, dl dd').each((_, el) => {
    if (el.name === 'dt')
      stats._k = $(el).text().trim().toLowerCase().replace(/[\s/]+/g, '_');
    else if (el.name === 'dd' && stats._k) {
      if (!stats[stats._k]) stats[stats._k] = $(el).text().trim();
      stats._k = null;
    }
  });

  let website =
    $('a[data-test-id="about-us__website"]').attr('href') ||
    $('[data-tracking-control-name="about_website"] a').attr('href') ||
    stats['website'] || null;
  if (website?.includes('linkedin.com/redir')) {
    try { website = new URL(website).searchParams.get('url') || website; } catch { /* keep */ }
  }

  const hq = pick($,
    '[data-test-id="about-us__headquarters"] dd',
    '[data-test-id="about-us__headquarters"]',
  ) || stats['headquarters'] || null;

  const industry = pick($,
    '[data-test-id="about-us__industry"] dd',
    '[data-test-id="about-us__industry"]',
  ) || stats['industry'] || null;

  const companyType = pick($,
    '[data-test-id="about-us__organizationType"] dd',
    '[data-test-id="about-us__organizationType"]',
  ) || stats['type'] || null;

  const founded = pick($,
    '[data-test-id="about-us__foundedOn"] dd',
    '[data-test-id="about-us__foundedOn"]',
  ) || stats['founded'] || null;

  const employeeCount = pick($,
    '[data-test-id="about-us__employeeCount"] dd',
    '[data-test-id="about-us__employeeCount"]',
    '.org-about-company-module__company-staff-count-range',
  ) || stats['company_size'] || null;

  const specialtiesRaw = pick($,
    '[data-test-id="about-us__specialties"] dd',
    '[data-test-id="about-us__specialties"]',
  ) || stats['specialties'] || '';
  const specialties = specialtiesRaw
    ? specialtiesRaw.split(',').map(s => s.trim()).filter(Boolean) : [];

  const followers = pick($,
    'span[data-test-id="followers-count"]',
    '[data-test-id="followers-count"]',
    '.org-top-card-summary-info-list__info-item',
  );

  const logo =
    $('img.org-top-card-primary-content__logo').attr('src') ||
    $('img[data-ghost-classes*="logo"]').attr('src') ||
    ogImage || null;

  const canonicalUrl = $('link[rel="canonical"]').attr('href') || sourceUrl;

  return { name, tagline, description, website, hq, industry, companyType,
    founded, employeeCount, specialties, followers, logo, linkedinUrl: canonicalUrl };
}

// ─────────────────────────────────────────────────────────────────────────────
//  PROXY HELPER
// ─────────────────────────────────────────────────────────────────────────────

let _proxyCfg = null;

async function initProxy(useProxy, countryCode) {
  if (!useProxy) return;
  for (const groups of [['RESIDENTIAL'], ['DATACENTER'], []]) {
    try {
      _proxyCfg = await Actor.createProxyConfiguration({
        groups: groups.length ? groups : undefined,
        countryCode,
      });
      console.log(`  Proxy init: ${groups[0] || 'AUTO'} (${countryCode}) ✓`);
      return;
    } catch { /* try next */ }
  }
  console.warn('  ⚠ Proxy init failed — running without proxy');
}

async function proxyUrl(tag = '') {
  if (!_proxyCfg) return null;
  try { return await _proxyCfg.newUrl(`li_${tag}_${Date.now()}`); }
  catch { return null; }
}

// ─────────────────────────────────────────────────────────────────────────────
//  MAIN
// ─────────────────────────────────────────────────────────────────────────────

await Actor.init();

const input = (await Actor.getInput()) ?? {};
const {
  salesNavUrl          = '',
  keywordsOverride     = null,
  maxCompanies         = 30,
  maxSearchPages       = 5,
  requestDelayMs       = 6000,
  deepScrape           = true,
  useApifyProxy        = true,
  proxyCountryCode     = 'IN',
  enableGoogleFallback = true,
} = input;

if (!salesNavUrl.trim()) {
  console.error('❌  No salesNavUrl provided. Exiting.');
  await Actor.exit();
}

// ── STEP 1: DECODE ─────────────────────────────────────────────────────────

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

await Actor.setValue('DECODED_FILTERS', filters);

const metaOnlyFilters = [];
if (filters.revenue.min !== null)       metaOnlyFilters.push('ANNUAL_REVENUE');
if (filters.headcountGrowth.length)     metaOnlyFilters.push('HEADCOUNT_GROWTH');
if (filters.fortune.length)             metaOnlyFilters.push('FORTUNE');
if (filters.technologiesUsed.length)    metaOnlyFilters.push('TECHNOLOGIES_USED');
if (filters.spotlight.length)           metaOnlyFilters.push('SPOTLIGHT');
if (metaOnlyFilters.length) console.log(`  Meta-only (not searchable): ${metaOnlyFilters.join(', ')}`);

await initProxy(useApifyProxy, proxyCountryCode);

// ── STEP 2: DISCOVER SLUGS ──────────────────────────────────────────────────

console.log('\n🔍  STEP 2 — Discovering company slugs…');
const slugMap = new Map();

// ── A: Typeahead ──────────────────────────────────────────────────────────────
console.log('\n  [A] LinkedIn Typeahead API (no auth)…');
const kwVariants = generateKeywordVariants(filters);
console.log(`    Variants: ${kwVariants.join(' | ')}`);

for (const kw of kwVariants) {
  if (slugMap.size >= maxCompanies) break;
  const url = buildTypeaheadUrl(kw, 10);
  const { body, ok, status } = await get(url, typeaheadHeaders(), await proxyUrl('ta'));

  if (kw === kwVariants[0]) {
    await Actor.setValue('debug_typeahead_p0', body?.slice(0, 3000) || `HTTP ${status}`);
  }

  if (!ok || !body) { console.warn(`    "${kw}": HTTP ${status}`); await sleep(randInt(2000, 4000)); continue; }

  const cos = parseTypeaheadResponse(body);
  console.log(`    "${kw}" → ${cos.length} result(s)`);
  for (const co of cos) { if (slugMap.size < maxCompanies && !slugMap.has(co.slug)) slugMap.set(co.slug, co); }
  await sleep(randInt(requestDelayMs / 3, requestDelayMs / 2));
}
console.log(`    After A: ${slugMap.size} companies`);

// ── B: HTML search ─────────────────────────────────────────────────────────────
if (slugMap.size < maxCompanies) {
  console.log('\n  [B] LinkedIn HTML search…');
  for (let page = 1; page <= maxSearchPages && slugMap.size < maxCompanies; page++) {
    const url = buildPublicSearchUrl(filters, page);
    console.log(`    Page ${page}: ${url.slice(0, 110)}`);

    const { body, ok, status } = await get(url, htmlHeaders('https://www.google.com/'), await proxyUrl(`srp${page}`));
    if (!ok || !body) { console.warn(`    HTTP ${status}`); await sleep(randInt(requestDelayMs, requestDelayMs + 2000)); continue; }

    const { slugs, isAuthWall } = extractSlugsFromHtml(body);
    const clean = slugs.filter(s => !SLUG_SKIP.has(s) && s.length > 2);
    console.log(`    ${clean.length} slug(s) (${isAuthWall ? 'AUTH WALL' : 'ok'})`);

    if (isAuthWall) {
      await Actor.setValue(`debug_html_srp_p${page}`, body, { contentType: 'text/html' });
      break;
    }

    for (const s of clean) {
      if (slugMap.size >= maxCompanies) break;
      if (!slugMap.has(s)) slugMap.set(s, { slug: s, linkedinUrl: `https://www.linkedin.com/company/${s}/about/` });
    }
    if (page === 1) await Actor.setValue('debug_html_srp_p1', body, { contentType: 'text/html' });
    await sleep(randInt(requestDelayMs, requestDelayMs + 2000));
  }
  console.log(`    After B: ${slugMap.size} companies`);
}

// ── C: Google/Bing SERP ────────────────────────────────────────────────────────
if (slugMap.size < maxCompanies && enableGoogleFallback) {
  console.log('\n  [C] Google/Bing SERP search…');
  const query = buildGoogleQuery(filters);
  console.log(`    Query: "${query}"`);

  const gHeaders = {
    'User-Agent': randUA(),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.google.com/',
    'DNT': '1',
  };

  for (let page = 1; page <= 3 && slugMap.size < maxCompanies; page++) {
    // Try Google first
    let slugs = [];
    const gUrl = buildGoogleUrl(query, page);
    const { body: gBody, ok: gOk } = await get(gUrl, gHeaders, await proxyUrl(`g${page}`));
    if (gOk && gBody) {
      slugs = extractSlugsFromSerp(gBody);
      console.log(`    Google p${page}: ${slugs.length} slugs`);
    } else {
      // Bing fallback
      const bUrl = buildBingUrl(query, page);
      const { body: bBody, ok: bOk } = await get(bUrl, { ...gHeaders, Referer: 'https://www.bing.com/' }, await proxyUrl(`b${page}`));
      if (bOk && bBody) {
        slugs = extractSlugsFromSerp(bBody);
        console.log(`    Bing p${page}: ${slugs.length} slugs`);
      }
    }
    for (const s of slugs) {
      if (slugMap.size >= maxCompanies) break;
      if (!slugMap.has(s)) slugMap.set(s, { slug: s, linkedinUrl: `https://www.linkedin.com/company/${s}/about/` });
    }
    await sleep(randInt(requestDelayMs / 2, requestDelayMs));
  }
  console.log(`    After C: ${slugMap.size} companies`);
}

console.log(`\n📦  Total companies queued: ${slugMap.size}`);

if (slugMap.size === 0) {
  console.error('\n❌  Zero companies found across all 3 strategies.\n');
  console.error('  ROOT CAUSE: Your proxy IPs are fully blocked by LinkedIn AND Google/Bing.\n');
  console.error('  SOLUTIONS:');
  console.error('  1. [Best] Upgrade Apify plan → enable RESIDENTIAL proxies');
  console.error('  2. Set proxyCountryCode to match your target region (IN for India)');
  console.error('  3. Increase requestDelayMs to 12000+');
  console.error('  4. Check debug_typeahead_p0 in Key-Value Store:');
  console.error('     - If it shows HTML: proxy is redirecting to auth wall');
  console.error('     - If it shows {}: empty JSON, endpoint changed');
  console.error('     - If it shows 40x: need session cookies (paid feature)');
  await Actor.exit();
}

// ── STEP 3: DEEP SCRAPE /about/ PAGES ─────────────────────────────────────

const filterContext = {
  keywords:         filters.keywords,
  headcounts:       filters.headcounts.map(h => h.label),
  headcountGrowth:  filters.headcountGrowth.map(h => h.label),
  industries:       filters.industries.map(i => i.text),
  regions:          filters.regions.map(r => r.text),
  companyTypes:     filters.companyTypes.map(c => c.label),
  revenue:          filters.revenue.min !== null
    ? `${filters.revenue.min}M–${filters.revenue.max ?? '∞'}M ${filters.revenue.currency}` : null,
  fortune:          filters.fortune,
  technologiesUsed: filters.technologiesUsed.map(t => t.text),
  metaOnlyFilters,
};

const companyList = [...slugMap.values()];
console.log(`\n🏢  STEP 3 — ${deepScrape ? 'Deep-scraping' : 'Saving (shallow)'} ${companyList.length} companies…`);

for (let ci = 0; ci < companyList.length; ci++) {
  const base = companyList[ci];
  const { slug } = base;
  const aboutUrl = `https://www.linkedin.com/company/${slug}/about/`;

  console.log(`\n  [${ci + 1}/${companyList.length}] ${slug}`);

  if (!deepScrape) {
    await Actor.pushData({ ...base, scrapeMode: 'shallow', filterContext, scrapedAt: new Date().toISOString() });
    continue;
  }

  const { body, ok, status } = await get(aboutUrl, htmlHeaders('https://www.linkedin.com/'), await proxyUrl(slug.slice(0, 8)));

  if (!body) {
    await Actor.pushData({ ...base, error: 'Fetch failed', httpStatus: status, scrapeMode: 'deep_failed', filterContext, scrapedAt: new Date().toISOString() });
    continue;
  }

  if (detectAuthWall(body)) {
    console.warn(`    ⚠ Auth wall`);
    await Actor.pushData({ ...base, error: 'Auth wall on /about/ page', httpStatus: status, scrapeMode: 'deep_blocked', filterContext, scrapedAt: new Date().toISOString() });
    await sleep(randInt(requestDelayMs * 2, requestDelayMs * 3));
    continue;
  }

  const jsonLd = parseJsonLd(body);
  const pg = scrapeCompanyAbout(body, aboutUrl);

  const record = {
    name:          jsonLd?.name          || base.name        || pg.name,
    tagline:       pg.tagline,
    description:   jsonLd?.description  || pg.description   || base.description,
    website:       jsonLd?.url           || pg.website,
    email:         jsonLd?.email         || null,
    phone:         jsonLd?.telephone     || null,
    headquarters:  pg.hq                || base.headquarters || jsonLd?.address?.addressLocality || null,
    streetAddress: jsonLd?.address?.streetAddress  || null,
    city:          jsonLd?.address?.addressLocality || null,
    stateRegion:   jsonLd?.address?.addressRegion   || null,
    country:       jsonLd?.address?.addressCountry  || null,
    postalCode:    jsonLd?.address?.postalCode       || null,
    fullAddress:   jsonLd?.address
      ? [jsonLd.address.streetAddress, jsonLd.address.addressLocality,
         jsonLd.address.addressRegion, jsonLd.address.postalCode,
         jsonLd.address.addressCountry].filter(Boolean).join(', ')
      : null,
    industry:      pg.industry          || base.industry,
    companyType:   pg.companyType,
    founded:       pg.founded           || jsonLd?.foundingDate || null,
    employeeCount: pg.employeeCount     || base.employeeRange,
    specialties:   pg.specialties,
    followers:     pg.followers         || (base.followerCount ? String(base.followerCount) : null),
    logo:          pg.logo              || base.logo           || jsonLd?.logo || null,
    linkedinUrl:   pg.linkedinUrl       || aboutUrl,
    sameAs:        jsonLd?.sameAs       || [],
    filterContext,
    revenueFilterApplied: filters.revenue.min !== null
      ? { min: filters.revenue.min, max: filters.revenue.max, currency: filters.revenue.currency,
          note: 'SalesNav search filter — NOT confirmed company revenue' }
      : null,
    headcountGrowthFilterApplied: filters.headcountGrowth.length
      ? filters.headcountGrowth.map(h => h.label) : null,
    slug,
    httpStatus:  status,
    scrapeMode:  'deep',
    scrapedAt:   new Date().toISOString(),
  };

  console.log(`    ✅ ${record.name || '(no name)'} | ${record.industry || 'n/a'} | ${record.employeeCount || 'n/a'}`);
  await Actor.pushData(record);

  if (ci < companyList.length - 1) await sleep(randInt(requestDelayMs, requestDelayMs + 2500));
}

const ds = await Actor.openDataset();
const { itemCount } = await ds.getInfo();
console.log(`\n🏁  Done! ${itemCount} records saved.`);
console.log('    Export: Apify Console → Storage → Datasets → Export CSV / JSON / Excel');

await Actor.exit();
