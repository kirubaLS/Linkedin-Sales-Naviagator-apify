/**
 * ═══════════════════════════════════════════════════════════════════════════
 *  LinkedIn SalesNav → Public Company Scraper  |  Apify Actor
 *  Node 18 · No login · No cookies · Educational use only
 * ═══════════════════════════════════════════════════════════════════════════
 *
 *  THE ARCHITECTURE ("cheat code"):
 *  ─────────────────────────────────
 *  Sales Navigator URLs carry ALL your filter choices encoded inside the
 *  ?query= param as a LinkedIn-proprietary mini-language, e.g.:
 *
 *    /sales/search/company?query=(
 *      filters:List(
 *        (type:COMPANY_HEADCOUNT, values:List((id:D,...))),
 *        (type:INDUSTRY,          values:List((id:96,...))),
 *        (type:REGION,            values:List((id:102257491,...))),
 *        (type:ANNUAL_REVENUE,    rangeValue:(min:1,max:10),
 *                                 selectedSubFilter:1)   ← USD
 *      ),
 *      keywords:saas,
 *      spellCorrectionEnabled:true
 *    )
 *
 *  This actor:
 *   1. DECODES  every filter type (headcount, industry, region, revenue,
 *               company type, growth, technologies, fortune, followers,
 *               spotlight, and more) out of the raw URL string.
 *   2. REMAPS   those filters onto LinkedIn's public (no-auth) company
 *               search endpoint parameters.
 *   3. SEARCHES the public endpoint page-by-page, collecting company URLs.
 *   4. SCRAPES  each /company/slug/about/ page via JSON-LD + Cheerio.
 *   5. OUTPUTS  every company as a rich structured record to Apify Dataset.
 *
 *  ⚠  Educational use only. Respect LinkedIn's Terms of Service.
 */

import { Actor } from 'apify';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

// ─────────────────────────────────────────────────────────────────────────────
//  LOOKUP TABLES
// ─────────────────────────────────────────────────────────────────────────────

/**
 * All SalesNav COMPANY_HEADCOUNT bucket IDs and their human-readable labels.
 * LinkedIn's internal letters map to these employee ranges.
 */
const HEADCOUNT_LABELS = {
  A: '1-10',
  B: '11-50',
  C: '51-200',
  D: '51-200',   // SalesNav "D" = same bucket in some regions
  E: '201-500',
  F: '501-1000',
  G: '1001-5000',
  H: '5001-10000',
  I: '10001+',
};

/**
 * SalesNav → LinkedIn public search "companySize" codes.
 * LinkedIn public search uses: A,B,C,D,E,F,G,H (different scale from SalesNav).
 */
const HEADCOUNT_TO_PUBLIC_SIZE = {
  A: 'A',   // 1-10    → public A
  B: 'B',   // 11-50   → public B
  C: 'C',   // 51-200  → public C
  D: 'C',   // 51-200  → public C (SalesNav duplicate)
  E: 'D',   // 201-500 → public D
  F: 'E',   // 501-1K  → public E
  G: 'F',   // 1K-5K   → public F
  H: 'G',   // 5K-10K  → public G
  I: 'H',   // 10K+    → public H
};

/**
 * All known SalesNav industry IDs mapped to their text names.
 * These numeric IDs are stable LinkedIn industry identifiers.
 */
const INDUSTRY_MAP = {
  '1':   'Defense and Space Manufacturing',
  '3':   'Computer Hardware Manufacturing',
  '4':   'Software Development',
  '5':   'Computer Networking Products',
  '6':   'Technology, Information and Internet',
  '7':   'Telecommunications',
  '8':   'Semiconductor Manufacturing',
  '9':   'Computers and Electronics Manufacturing',
  '10':  'Transportation Equipment Manufacturing',
  '11':  'Higher Education',
  '12':  'Primary and Secondary Education',
  '13':  'Education Administration Programs',
  '14':  'Research Services',
  '15':  'Armed Forces',
  '16':  'Legislative Offices',
  '17':  'Administration of Justice',
  '18':  'International Affairs',
  '19':  'Government Administration',
  '20':  'Executive Offices',
  '21':  'Law Practice',
  '22':  'Legal Services',
  '23':  'Management Consulting',
  '24':  'IT Services and IT Consulting',
  '25':  'Facilities Services',
  '26':  'Civil Engineering',
  '27':  'Architecture and Planning',
  '28':  'Construction',
  '29':  'Wholesale Building Materials',
  '30':  'Real Estate',
  '31':  'Retail Groceries',
  '32':  'Retail',
  '33':  'Wholesale',
  '34':  'Food and Beverage Manufacturing',
  '35':  'Food and Beverage Retail',
  '36':  'Beverage Manufacturing',
  '37':  'Tobacco Manufacturing',
  '38':  'Textile Manufacturing',
  '39':  'Leather Product Manufacturing',
  '40':  'Apparel Manufacturing',
  '41':  'Medical Practices',
  '42':  'Hospitals and Health Care',
  '43':  'Pharmaceutical Manufacturing',
  '44':  'Biotechnology Research',
  '45':  'Veterinary Services',
  '46':  'Medical Equipment Manufacturing',
  '47':  'Embedded Systems',
  '48':  'Machinery Manufacturing',
  '49':  'Automation Machinery Manufacturing',
  '50':  'Industrial Machinery Manufacturing',
  '51':  'Transportation Programs',
  '52':  'Aviation and Aerospace Component Manufacturing',
  '53':  'Airlines and Aviation',
  '54':  'Maritime Transportation',
  '55':  'Rail Transportation',
  '56':  'Truck Transportation',
  '57':  'Courier and Package Delivery',
  '58':  'Warehousing and Storage',
  '59':  'Packaging and Containers Manufacturing',
  '60':  'Chemicals Manufacturing',
  '61':  'Consumer Goods Rental',
  '62':  'Consumer Services',
  '63':  'Entertainment Providers',
  '64':  'Broadcast Media Production and Distribution',
  '65':  'Movies, Videos and Sound',
  '66':  'Animation and Post-production',
  '67':  'Media and Telecommunications',
  '68':  'Book and Periodical Publishing',
  '69':  'Newspaper Publishing',
  '70':  'Periodical Publishing',
  '71':  'Advertising Services',
  '72':  'Marketing Services',
  '73':  'Public Relations and Communications Services',
  '74':  'Printing Services',
  '75':  'Personal Care Product Manufacturing',
  '76':  'Cosmetics',
  '77':  'Luxury Goods and Jewelry',
  '78':  'Glass, Ceramics and Concrete Manufacturing',
  '79':  'Paper and Forest Product Manufacturing',
  '80':  'Plastics and Rubber Products Manufacturing',
  '81':  'Metal Ore Mining',
  '82':  'Oil and Gas',
  '83':  'Coal Mining',
  '84':  'Mining',
  '85':  'Environmental Services',
  '86':  'Strategic Management Services',
  '87':  'Business Consulting and Services',
  '88':  'Human Resources Services',
  '89':  'Staffing and Recruiting',
  '90':  'Outsourcing and Offshoring Consulting',
  '91':  'Financial Services',
  '92':  'Investment Banking',
  '93':  'Investment Management',
  '94':  'Accounting',
  '95':  'Insurance',
  '96':  'Banking',
  '97':  'Venture Capital and Private Equity Principals',
  '98':  'Capital Markets',
  '99':  'Credit Intermediation',
  '100': 'Leasing Real Property',
  '101': 'Religious Institutions',
  '102': 'Individual and Family Services',
  '103': 'Community Services',
  '104': 'Civic and Social Organizations',
  '105': 'Non-profit Organizations',
  '106': 'Philanthropic Fundraising Services',
  '107': 'Recreational Facilities',
  '108': 'Spectator Sports',
  '109': 'Sports and Recreation Instruction',
  '110': 'Wellness and Fitness Services',
  '111': 'Amusement Parks and Arcades',
  '112': 'Musicians',
  '113': 'Artists and Writers',
  '114': 'Performing Arts',
  '115': 'Photography',
  '116': 'Design Services',
  '117': 'Graphic Design',
  '118': 'Interior Design',
  '119': 'Architecture and Planning',
  '120': 'Landscape Architecture',
  '121': 'E-Learning Providers',
  '122': 'Computer Games',
  '123': 'Mobile Gaming Apps',
  '124': 'Online Audio and Video Media',
  '125': 'Internet Marketplace Platforms',
  '126': 'IT System Data Services',
  '127': 'Internet Publishing',
  '128': 'Hospitals and Health Care',
  '129': 'Mental Health Care',
  '130': 'Alternative Medicine',
  '131': 'Physical, Occupational and Speech Therapists',
  '132': 'Nanotechnology Research',
  '133': 'Renewable Energy Semiconductor Manufacturing',
  '134': 'Electric Power Generation',
  '135': 'Oil, Gas, and Mining',
  '136': 'Utilities',
  '137': 'Technology, Information and Media',
  '138': 'Data Infrastructure and Analytics',
  '139': 'Computer and Network Security',
  '140': 'Artificial Intelligence',
  '141': 'Blockchain Services',
  '142': 'Information Services',
  '143': 'Data Security Software Products',
  '144': 'Cybersecurity',
};

/**
 * ANNUAL_REVENUE selectedSubFilter IDs → currency codes.
 * These are LinkedIn's internal currency identifiers.
 */
const REVENUE_CURRENCY_MAP = {
  '1':  'USD',
  '2':  'EUR',
  '3':  'GBP',
  '4':  'INR',
  '5':  'CAD',
  '6':  'AUD',
  '7':  'CNY',
  '8':  'JPY',
  '9':  'BRL',
  '10': 'MXN',
  '11': 'SGD',
  '12': 'CHF',
  '13': 'SEK',
  '14': 'NOK',
  '15': 'DKK',
  '16': 'PLN',
  '17': 'ZAR',
  '18': 'AED',
  '19': 'SAR',
  '20': 'HKD',
  '21': 'KRW',
};

/**
 * COMPANY_TYPE filter IDs.
 */
const COMPANY_TYPE_MAP = {
  'C': 'Public Company',
  'D': 'Privately Held',
  'E': 'Non-profit',
  'B': 'Partnership',
  'A': 'Self-Employed',
  'F': 'Government Agency',
  'G': 'Educational Institution',
  'H': 'Sole Proprietorship',
};

/**
 * HEADCOUNT_GROWTH filter IDs → percentage ranges.
 */
const HEADCOUNT_GROWTH_MAP = {
  'ABOVE_40': 'Above 40% growth',
  'BETWEEN_20_AND_40': '20%-40% growth',
  'BETWEEN_10_AND_20': '10%-20% growth',
  'BETWEEN_5_AND_10': '5%-10% growth',
  'BETWEEN_0_AND_5': '0%-5% growth',
  'NEGATIVE': 'Negative growth (decline)',
};

/**
 * FORTUNE filter IDs.
 */
const FORTUNE_MAP = {
  '10':  'Fortune 10',
  '50':  'Fortune 50',
  '100': 'Fortune 100',
  '500': 'Fortune 500',
};

/**
 * FOLLOWERS_OF filter IDs → follower count ranges (LinkedIn's buckets).
 */
const FOLLOWERS_MAP = {
  'A': '1-100 followers',
  'B': '101-1K followers',
  'C': '1K-5K followers',
  'D': '5K-10K followers',
  'E': '10K-50K followers',
  'F': '50K+ followers',
};

// User-agents pool for realistic browser impersonation
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
];

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 1 ── DECODE SALES NAVIGATOR URL
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fully decode a LinkedIn Sales Navigator URL into a structured filter object.
 *
 * Handles all known filter types:
 *   COMPANY_HEADCOUNT, INDUSTRY, REGION, GEOGRAPHY, ANNUAL_REVENUE,
 *   COMPANY_TYPE, HEADCOUNT_GROWTH, FORTUNE, FOLLOWERS_OF,
 *   TECHNOLOGIES_USED, SPOTLIGHT, DEPARTMENT_HEADCOUNT,
 *   DEPARTMENT_HEADCOUNT_GROWTH, CURRENT_COMPANY, PAST_COMPANY,
 *   and more.
 *
 * Also extracts top-level fields: keywords, spellCorrectionEnabled,
 * savedSearchId, sessionId.
 */
function decodeSalesNavUrl(rawUrl) {
  // URL-decode the whole string once so we work with human-readable text
  let decoded;
  try {
    decoded = decodeURIComponent(rawUrl);
  } catch {
    decoded = rawUrl;
  }

  const result = {
    // ── Core search fields ──────────────────────────────────────────────
    keywords:              null,
    savedSearchId:         null,
    sessionId:             null,
    searchType:            null,   // 'company' | 'people'

    // ── Company / Account filters ────────────────────────────────────────
    headcounts:            [],   // [{id, label}]
    headcountGrowth:       [],   // [{id, label}]
    industries:            [],   // [{id, text}]
    regions:               [],   // [{id, text}]
    companyTypes:          [],   // [{id, label}]
    revenue: {
      min:      null,            // number in millions
      max:      null,
      currency: 'USD',
    },
    fortune:               [],   // e.g. ['Fortune 500']
    followers:             [],   // [{id, label}]
    technologiesUsed:      [],   // [{id, text}]
    departmentHeadcount:   [],   // [{dept, id, label}]
    departmentGrowth:      [],   // [{dept, id, label}]
    spotlight:             [],   // [{id, text}]
    currentCompany:        [],   // [{id, text}]
    pastCompany:           [],   // [{id, text}]
    postalCodes:           [],   // [string]

    // ── People / Lead filters (carried along for context) ─────────────────
    jobTitles:             [],
    seniorityLevels:       [],
    functions:             [],
    yearsInRole:           [],
    yearsAtCompany:        [],
    yearsOfExperience:     [],
    schools:               [],
    groups:                [],
    profileLanguages:      [],

    // ── Raw data for debugging ───────────────────────────────────────────
    rawDecoded:            decoded,
    unknownFilters:        [],
  };

  // Detect search type from URL path
  if (decoded.includes('/sales/search/company')) result.searchType = 'company';
  else if (decoded.includes('/sales/search/people')) result.searchType = 'people';

  // ── Top-level scalar fields ────────────────────────────────────────────────
  const kwMatch = decoded.match(/[,(?]keywords:([^,)&\n]+)/);
  if (kwMatch) result.keywords = safeDecodeUri(kwMatch[1].trim());

  const savedMatch = decoded.match(/savedSearchId[=:](\d+)/);
  if (savedMatch) result.savedSearchId = savedMatch[1];

  const sessionMatch = decoded.match(/sessionId[=:]([^&)\n]+)/);
  if (sessionMatch) result.sessionId = sessionMatch[1];

  // ── Revenue: special non-list filter ──────────────────────────────────────
  // Pattern: (type:ANNUAL_REVENUE,rangeValue:(min:X,max:Y),selectedSubFilter:Z)
  // Note: LinkedIn also emits this without wrapping parens in some versions
  const revMatch = decoded.match(
    /type:ANNUAL_REVENUE[^)]*rangeValue:\(min:(\d+(?:\.\d+)?),max:(\d+(?:\.\d+)?)\)(?:[^)]*selectedSubFilter:(\d+))?/
  );
  if (revMatch) {
    result.revenue.min      = parseFloat(revMatch[1]);
    result.revenue.max      = parseFloat(revMatch[2]);
    result.revenue.currency = REVENUE_CURRENCY_MAP[revMatch[3]] || 'USD';
  }

  // Alternate pattern: rangeValue:(min:X) only (open-ended)
  const revMinOnly = decoded.match(/type:ANNUAL_REVENUE[^)]*rangeValue:\(min:(\d+(?:\.\d+)?)\)/);
  if (revMinOnly && result.revenue.min === null) {
    result.revenue.min      = parseFloat(revMinOnly[1]);
    result.revenue.currency = 'USD';
  }

  // ── Department headcount: (type:DEPARTMENT_HEADCOUNT,selectedSubFilter:N,values:...)
  const deptHcRegex = /type:DEPARTMENT_HEADCOUNT,selectedSubFilter:(\d+),values:List\(([^)]*(?:\([^)]*\)[^)]*)*)\)/g;
  let deptM;
  while ((deptM = deptHcRegex.exec(decoded)) !== null) {
    const deptId = deptM[1];
    extractValues(deptM[2]).forEach(v => {
      result.departmentHeadcount.push({
        dept:  deptId,
        id:    v.id,
        label: v.text || HEADCOUNT_LABELS[v.id] || v.id,
      });
    });
  }

  // ── Department headcount growth ────────────────────────────────────────────
  const deptGrowthRegex = /type:DEPARTMENT_HEADCOUNT_GROWTH,selectedSubFilter:(\d+),values:List\(([^)]*(?:\([^)]*\)[^)]*)*)\)/g;
  let dgM;
  while ((dgM = deptGrowthRegex.exec(decoded)) !== null) {
    const deptId = dgM[1];
    extractValues(dgM[2]).forEach(v => {
      result.departmentGrowth.push({
        dept:  deptId,
        id:    v.id,
        label: v.text || HEADCOUNT_GROWTH_MAP[v.id] || v.id,
      });
    });
  }

  // ── All standard (type:X, values:List(...)) filters ────────────────────────
  // We match the outer filter block then parse its values sub-list
  const filterBlockRegex = /\(type:([\w_]+),values:List\(([^()]*(?:\([^()]*\)[^()]*)*)\)(?:,selectionType:\w+)?\)/g;
  let fm;
  while ((fm = filterBlockRegex.exec(decoded)) !== null) {
    const fType  = fm[1];
    const vBlock = fm[2];
    const vals   = extractValues(vBlock);

    switch (fType) {
      case 'COMPANY_HEADCOUNT':
        vals.forEach(v => result.headcounts.push({
          id:    v.id,
          label: v.text || HEADCOUNT_LABELS[v.id] || v.id,
          selectionType: v.selectionType,
        }));
        break;

      case 'HEADCOUNT_GROWTH':
        vals.forEach(v => result.headcountGrowth.push({
          id:    v.id,
          label: v.text || HEADCOUNT_GROWTH_MAP[v.id] || v.id,
          selectionType: v.selectionType,
        }));
        break;

      case 'INDUSTRY':
        vals.forEach(v => result.industries.push({
          id:   v.id,
          text: v.text || INDUSTRY_MAP[v.id] || `Industry ${v.id}`,
          selectionType: v.selectionType,
        }));
        break;

      case 'REGION':
      case 'GEOGRAPHY':
        vals.forEach(v => result.regions.push({
          id:   v.id,
          text: v.text || v.id,
          selectionType: v.selectionType,
        }));
        break;

      case 'COMPANY_TYPE':
        vals.forEach(v => result.companyTypes.push({
          id:    v.id,
          label: v.text || COMPANY_TYPE_MAP[v.id] || v.id,
          selectionType: v.selectionType,
        }));
        break;

      case 'FORTUNE':
        vals.forEach(v => result.fortune.push(
          v.text || FORTUNE_MAP[v.id] || `Fortune ${v.id}`
        ));
        break;

      case 'FOLLOWERS_OF':
        vals.forEach(v => result.followers.push({
          id:    v.id,
          label: v.text || FOLLOWERS_MAP[v.id] || v.id,
        }));
        break;

      case 'TECHNOLOGIES_USED':
        vals.forEach(v => result.technologiesUsed.push({
          id:   v.id,
          text: v.text || v.id,
        }));
        break;

      case 'SPOTLIGHT':
        vals.forEach(v => result.spotlight.push({
          id:   v.id,
          text: v.text || v.id,
        }));
        break;

      case 'CURRENT_COMPANY':
        vals.forEach(v => result.currentCompany.push({
          id:   v.id,
          text: v.text || v.id,
        }));
        break;

      case 'PAST_COMPANY':
        vals.forEach(v => result.pastCompany.push({
          id:   v.id,
          text: v.text || v.id,
        }));
        break;

      case 'TITLE':
      case 'JOB_TITLE':
        vals.forEach(v => result.jobTitles.push(v.text || v.id));
        break;

      case 'SENIORITY_LEVEL':
        vals.forEach(v => result.seniorityLevels.push(v.text || v.id));
        break;

      case 'FUNCTION':
        vals.forEach(v => result.functions.push(v.text || v.id));
        break;

      case 'YEARS_IN_CURRENT_POSITION':
        vals.forEach(v => result.yearsInRole.push(v.text || v.id));
        break;

      case 'YEARS_AT_CURRENT_COMPANY':
        vals.forEach(v => result.yearsAtCompany.push(v.text || v.id));
        break;

      case 'YEARS_OF_EXPERIENCE':
        vals.forEach(v => result.yearsOfExperience.push(v.text || v.id));
        break;

      case 'SCHOOL':
        vals.forEach(v => result.schools.push(v.text || v.id));
        break;

      case 'LINKEDIN_GROUP':
        vals.forEach(v => result.groups.push(v.text || v.id));
        break;

      case 'PROFILE_LANGUAGE':
        vals.forEach(v => result.profileLanguages.push(v.text || v.id));
        break;

      // Postal codes come as simple id strings
      case 'POSTAL_CODE':
        vals.forEach(v => result.postalCodes.push(v.id));
        break;

      default:
        // Capture unknown filters for transparency
        if (!['recentSearchParam','spellCorrectionEnabled'].includes(fType)) {
          result.unknownFilters.push({ type: fType, values: vals });
        }
    }
  }

  return result;
}

/**
 * Parse a List(...) value block into an array of {id, text, selectionType} objects.
 * Handles nested parens for values that contain sub-objects.
 *
 * Examples of what we parse:
 *   (id:D,text:51-200,selectionType:INCLUDED)
 *   (id:urn%3Ali%3Aorganization%3A1441,text:Google,selectionType:INCLUDED)
 *   (id:102257491,text:United+States,selectionType:INCLUDED,parent:(id:0))
 */
function extractValues(block) {
  const values = [];
  // Simple state machine: track paren depth to find top-level (…) entries
  let depth = 0;
  let start = -1;
  for (let i = 0; i < block.length; i++) {
    if (block[i] === '(') {
      depth++;
      if (depth === 1) start = i;
    } else if (block[i] === ')') {
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

/** Safe URI decoder — never throws on malformed sequences */
function safeDecodeUri(s) {
  if (!s) return s;
  try {
    return decodeURIComponent(s).replace(/\+/g, ' ');
  } catch {
    // Fall back: replace + with space, leave %XX as-is
    return s.replace(/\+/g, ' ');
  }
}

function parseValueEntry(entry) {
  const obj = { id: null, text: null, selectionType: 'INCLUDED' };

  const idM   = entry.match(/(?:^|,)id:([^,)]+)/);
  const textM = entry.match(/(?:^|,)text:([^,(]+)/);
  const selM  = entry.match(/selectionType:(\w+)/);

  if (idM)   obj.id            = safeDecodeUri(idM[1].trim());
  if (textM) obj.text          = safeDecodeUri(textM[1].trim());
  if (selM)  obj.selectionType = selM[1];

  return obj;
}

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 2 ── BUILD LINKEDIN PUBLIC SEARCH URLS
// ─────────────────────────────────────────────────────────────────────────────

const LI_SEARCH_BASE = 'https://www.linkedin.com/search/results/companies/';

/**
 * Translate decoded SalesNav filters into LinkedIn public company search URLs.
 *
 * LinkedIn's public search (linkedin.com/search/results/companies/) accepts:
 *   ?keywords=       → keyword search
 *   ?companySize=    → single size code (A-H)
 *   ?industry=       → numeric industry ID
 *   ?geoUrn=         → "urn:li:geo:ID" (same IDs as SalesNav regions)
 *   ?page=           → page number (10 results/page)
 *
 * Revenue, growth rate, technologies, fortune, etc. have NO equivalent in
 * the public search — we capture those as metadata only and note them in output.
 *
 * We cross-join size × industry combinations so each search stays focused,
 * then paginate up to maxPages per combination.
 */
function buildPublicSearchUrls(filters, maxPages) {
  const urls = new Set();

  // Keyword: prefer explicit keywords; fall back to industry names as search terms
  const kw =
    (filters.keywords && filters.keywords.trim()) ||
    (filters.industries.length > 0
      ? filters.industries.map(i => i.text).join(' ')
      : 'company');

  // Headcount → public size codes (deduplicate)
  const sizes = [
    ...new Set(
      filters.headcounts.length
        ? filters.headcounts
            .filter(h => h.selectionType !== 'EXCLUDED')
            .map(h => HEADCOUNT_TO_PUBLIC_SIZE[h.id] || null)
            .filter(Boolean)
        : [null] // null = no size filter
    ),
  ];

  // Industries (INCLUDED only)
  const industries = filters.industries.length
    ? filters.industries.filter(i => i.selectionType !== 'EXCLUDED').map(i => i.id)
    : [null];

  // Regions → geo URNs (LinkedIn public search accepts the same numeric IDs)
  const geoUrns = filters.regions.length
    ? filters.regions
        .filter(r => r.selectionType !== 'EXCLUDED')
        .map(r => `urn:li:geo:${r.id}`)
    : [];

  // Cross-join size × industry, paginate
  for (const size of sizes) {
    for (const industryId of industries) {
      for (let page = 1; page <= maxPages; page++) {
        const params = new URLSearchParams();
        params.set('keywords', kw);
        params.set('origin', 'FACETED_SEARCH');
        params.set('page', String(page));

        if (size)       params.set('companySize', size);
        if (industryId) params.set('industry', industryId);
        if (geoUrns.length > 0) params.set('geoUrn', geoUrns.join(','));

        urls.add(`${LI_SEARCH_BASE}?${params.toString()}`);
      }
    }
  }

  return [...urls];
}

// ─────────────────────────────────────────────────────────────────────────────
//  HTTP UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

const sleep = ms => new Promise(r => setTimeout(r, ms));
const randUA  = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

function makeHeaders(referer = 'https://www.google.com/') {
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

async function fetchPage(url, proxyUrl = null, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const opts = {
        url,
        headers:        makeHeaders('https://www.linkedin.com/'),
        followRedirect: true,
        timeout:        { request: 35_000 },
        retry:          { limit: 0 },
        https:          { rejectUnauthorized: false },
      };
      if (proxyUrl) opts.proxyUrl = proxyUrl;

      const res = await gotScraping(opts);

      if (res.statusCode === 200) return { html: res.body, ok: true,  status: 200 };

      if (res.statusCode === 429 || res.statusCode === 999) {
        const wait = randInt(8000, 15000);
        console.warn(`    ⚠ Rate-limit (${res.statusCode}), waiting ${wait}ms…`);
        await sleep(wait);
        continue;
      }

      console.warn(`    HTTP ${res.statusCode} → ${url.slice(0, 80)}`);
      return { html: res.body, ok: false, status: res.statusCode };

    } catch (e) {
      console.error(`    Attempt ${attempt}/${retries} error: ${e.message}`);
      if (attempt < retries) await sleep(randInt(2000, 5000));
    }
  }
  return { html: null, ok: false, status: 0 };
}

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 3 ── PARSE SEARCH RESULT PAGE → COLLECT COMPANY URLS
// ─────────────────────────────────────────────────────────────────────────────

function detectAuthWall(html) {
  return (
    html.includes('authwall') ||
    html.includes('/login?') ||
    html.includes('Join to see') ||
    html.includes('Sign in to view') ||
    html.includes('uas/authenticate') ||
    html.includes('checkpoint/challenge')
  );
}

function extractCompanyUrlsFromSearchPage(html) {
  const $ = cheerio.load(html);
  const found = new Set();

  // All <a> tags pointing to /company/* paths
  $('a[href*="/company/"]').each((_, el) => {
    const href = ($(el).attr('href') || '').split('?')[0].split('#')[0];
    const m = href.match(/\/company\/([a-zA-Z0-9_%-]+)/);
    if (!m) return;
    const slug = decodeURIComponent(m[1]).toLowerCase().replace(/\s+/g, '-');
    // Exclude sub-pages that aren't the root company page
    const SKIP = ['about', 'jobs', 'posts', 'people', 'insights', 'life', 'products', 'videos', 'events', 'mycompany'];
    if (!SKIP.includes(slug) && slug.length > 1) {
      found.add(`https://www.linkedin.com/company/${slug}/about/`);
    }
  });

  return { companies: [...found], isAuthWall: detectAuthWall(html) };
}

// ─────────────────────────────────────────────────────────────────────────────
//  STEP 4 ── SCRAPE INDIVIDUAL COMPANY /about/ PAGE
// ─────────────────────────────────────────────────────────────────────────────

function parseJsonLd(html) {
  try {
    const $ = cheerio.load(html);
    let best = null;
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const obj = JSON.parse($(el).html() || '{}');
        if (obj?.['@type'] === 'Organization' || obj?.name) {
          best = obj;
          return false; // stop after first match
        }
      } catch { /* skip malformed */ }
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

  // ── Name ────────────────────────────────────────────────────────────────
  const name = pick($,
    'h1.top-card-layout__title',
    'h1[class*="org-top-card"]',
    'h1[class*="artdeco"]',
    '.organization-outlet h1',
    'h1',
  );

  // ── Tagline ─────────────────────────────────────────────────────────────
  const tagline = pick($,
    'h4.top-card-layout__second-subline',
    'p.org-top-card-summary__tagline',
    '[data-test-id="about-us__tagline"]',
  );

  // ── Description ─────────────────────────────────────────────────────────
  const description = pick($,
    'section.core-section-container p.core-section-container__main-description',
    'p[data-test-id="about-us__description"]',
    '[data-test-id="about-us"] p',
    '.org-about-us-organization-description__text',
    '.break-words p',
    '.core-section-container__main-description',
  );

  // ── dt/dd stat pairs ─────────────────────────────────────────────────────
  const stats = {};
  $('dl dt, dl dd').each((_, el) => {
    if (el.name === 'dt') {
      stats._k = $(el).text().trim().toLowerCase().replace(/[\s/]+/g, '_');
    } else if (el.name === 'dd' && stats._k) {
      if (!stats[stats._k]) stats[stats._k] = $(el).text().trim();
      stats._k = null;
    }
  });

  // ── Website ──────────────────────────────────────────────────────────────
  let website =
    $('a[data-test-id="about-us__website"]').attr('href') ||
    $('[data-tracking-control-name="about_website"] a').attr('href') ||
    stats['website'] || null;
  if (website?.includes('linkedin.com/redir')) {
    try { website = new URL(website).searchParams.get('url') || website; } catch { /* keep */ }
  }

  // ── Other structured fields ───────────────────────────────────────────────
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
  ) || stats['type'] || stats['company_type'] || null;

  const founded = pick($,
    '[data-test-id="about-us__foundedOn"] dd',
    '[data-test-id="about-us__foundedOn"]',
  ) || stats['founded'] || null;

  const employeeCount = pick($,
    '[data-test-id="about-us__employeeCount"] dd',
    '[data-test-id="about-us__employeeCount"]',
    '.org-about-company-module__company-staff-count-range',
  ) || stats['company_size'] || stats['employees'] || null;

  const specialtiesRaw = pick($,
    '[data-test-id="about-us__specialties"] dd',
    '[data-test-id="about-us__specialties"]',
  ) || stats['specialties'] || '';

  const specialties = specialtiesRaw
    ? specialtiesRaw.split(',').map(s => s.trim()).filter(Boolean)
    : [];

  const followers = pick($,
    'span[data-test-id="followers-count"]',
    '.org-top-card-summary-info-list__info-item',
    '[data-test-id="followers-count"]',
  );

  const associatedMembers = $('a[href*="/search/results/people"]').first().text().trim() || null;

  const logo =
    $('img.org-top-card-primary-content__logo').attr('src') ||
    $('img[data-ghost-classes*="logo"]').attr('src') ||
    $('img[alt*="logo"]').first().attr('src') || null;

  const canonicalUrl = $('link[rel="canonical"]').attr('href') || sourceUrl;

  return {
    name, tagline, description, website, hq,
    industry, companyType, founded, employeeCount,
    specialties, followers, associatedMembers, logo,
    linkedinUrl: canonicalUrl,
    _stats: stats,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
//  PROXY HELPER
// ─────────────────────────────────────────────────────────────────────────────

async function getProxyUrl(useProxy, countryCode, tag = '') {
  if (!useProxy) return null;
  // Try residential first (best for LinkedIn); fall back to datacenter on free tier
  for (const groups of [['RESIDENTIAL'], []]) {
    try {
      const cfg = await Actor.createProxyConfiguration({ groups, countryCode });
      return await cfg.newUrl(`li_${tag}_${Date.now()}`);
    } catch { /* try next tier */ }
  }
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
//  MAIN
// ─────────────────────────────────────────────────────────────────────────────

await Actor.init();

const input = (await Actor.getInput()) ?? {};
const {
  salesNavUrl       = '',
  keywordsOverride  = null,
  maxCompanies      = 50,
  maxSearchPages    = 5,
  requestDelayMs    = 3500,
  deepScrape        = true,
  useApifyProxy     = true,
  proxyCountryCode  = 'US',
} = input;

if (!salesNavUrl.trim()) {
  console.error('❌  No salesNavUrl in input. Exiting.');
  await Actor.exit();
}

// ══════════════════════════════════════════════════════════════════════════════
//  STEP 1 — DECODE
// ══════════════════════════════════════════════════════════════════════════════

console.log('\n📋  STEP 1 — Decoding Sales Navigator URL…');
const filters = decodeSalesNavUrl(salesNavUrl);
if (keywordsOverride) filters.keywords = keywordsOverride;

// Pretty-print decoded filters
console.log(`  Search type       : ${filters.searchType || 'unknown'}`);
console.log(`  Keywords          : ${filters.keywords || '(none)'}`);
console.log(`  Headcounts        : ${filters.headcounts.map(h => h.label).join(', ') || '(any)'}`);
console.log(`  Headcount growth  : ${filters.headcountGrowth.map(h => h.label).join(', ') || '(none)'}`);
console.log(`  Industries        : ${filters.industries.map(i => i.text).join(', ') || '(any)'}`);
console.log(`  Regions           : ${filters.regions.map(r => r.text).join(', ') || '(any)'}`);
console.log(`  Company types     : ${filters.companyTypes.map(c => c.label).join(', ') || '(any)'}`);
console.log(`  Revenue           : ${filters.revenue.min !== null
  ? `${filters.revenue.min}M–${filters.revenue.max !== null ? filters.revenue.max + 'M' : '∞'} ${filters.revenue.currency}`
  : '(none)'}`);
console.log(`  Fortune           : ${filters.fortune.join(', ') || '(none)'}`);
console.log(`  Technologies      : ${filters.technologiesUsed.map(t => t.text).join(', ') || '(none)'}`);
console.log(`  Spotlight         : ${filters.spotlight.map(s => s.text).join(', ') || '(none)'}`);
console.log(`  Dept headcount    : ${filters.departmentHeadcount.length > 0 ? JSON.stringify(filters.departmentHeadcount) : '(none)'}`);
if (filters.unknownFilters.length) {
  console.log(`  Unknown filters   : ${filters.unknownFilters.map(f => f.type).join(', ')}`);
}

// Save full decoded filter map to KV store for reference / debugging
await Actor.setValue('DECODED_FILTERS', filters);

// Warn about filters we CAN decode but can't replicate in public search
const metaOnlyFilters = [];
if (filters.revenue.min !== null)        metaOnlyFilters.push('ANNUAL_REVENUE');
if (filters.headcountGrowth.length)      metaOnlyFilters.push('HEADCOUNT_GROWTH');
if (filters.fortune.length)              metaOnlyFilters.push('FORTUNE');
if (filters.followers.length)            metaOnlyFilters.push('FOLLOWERS_OF');
if (filters.technologiesUsed.length)     metaOnlyFilters.push('TECHNOLOGIES_USED');
if (filters.spotlight.length)            metaOnlyFilters.push('SPOTLIGHT');
if (filters.departmentHeadcount.length)  metaOnlyFilters.push('DEPARTMENT_HEADCOUNT');
if (filters.departmentGrowth.length)     metaOnlyFilters.push('DEPARTMENT_HEADCOUNT_GROWTH');

if (metaOnlyFilters.length) {
  console.log(`\n  ⚠  These filters have no equivalent in LinkedIn's public search:`);
  console.log(`     ${metaOnlyFilters.join(', ')}`);
  console.log(`     They are decoded and saved to output metadata but cannot narrow the public results.`);
}

// ══════════════════════════════════════════════════════════════════════════════
//  STEP 2 — BUILD SEARCH URLS
// ══════════════════════════════════════════════════════════════════════════════

console.log('\n🔗  STEP 2 — Building public search URLs…');
const searchUrls = buildPublicSearchUrls(filters, maxSearchPages);
console.log(`  Generated ${searchUrls.length} unique search URL(s)`);
if (searchUrls.length === 0) {
  console.error('  ❌  No search URLs generated. Check filter decoding.');
  await Actor.exit();
}

// ══════════════════════════════════════════════════════════════════════════════
//  STEP 3 — CRAWL SEARCH RESULTS
// ══════════════════════════════════════════════════════════════════════════════

console.log('\n🔍  STEP 3 — Crawling public search results…');
const companyUrlSet = new Set();

for (let si = 0; si < searchUrls.length; si++) {
  if (companyUrlSet.size >= maxCompanies) break;
  const searchUrl = searchUrls[si];
  console.log(`\n  [${si + 1}/${searchUrls.length}] ${searchUrl.slice(0, 100)}`);

  const proxy = await getProxyUrl(useApifyProxy, proxyCountryCode, `s${si}`);
  const { html, ok, status } = await fetchPage(searchUrl, proxy);

  if (!html || !ok) {
    console.warn(`    ⚠ Skipped (HTTP ${status})`);
    await sleep(randInt(requestDelayMs, requestDelayMs + 2000));
    continue;
  }

  const { companies, isAuthWall } = extractCompanyUrlsFromSearchPage(html);

  if (isAuthWall) {
    console.warn(`    ⚠ Auth wall — LinkedIn is blocking unauthenticated access on this IP.`);
    console.warn(`      Enable Apify Proxy (residential) for best results.`);
    await Actor.setValue(`debug_search_${si}_html`, html, { contentType: 'text/html' });
    await sleep(randInt(requestDelayMs * 2, requestDelayMs * 3));
    continue;
  }

  console.log(`    ✅ Found ${companies.length} company URL(s)`);
  companies.forEach(u => companyUrlSet.add(u));

  await sleep(randInt(requestDelayMs, requestDelayMs + 2000));
}

const companyUrls = [...companyUrlSet].slice(0, maxCompanies);
console.log(`\n📦  Total unique companies queued: ${companyUrls.length}`);

if (companyUrls.length === 0) {
  console.error('❌  No companies found. Common causes:');
  console.error('    1. LinkedIn is blocking requests — enable Apify Proxy');
  console.error('    2. Filter combination returns no public results');
  console.error('    3. Sales Nav URL could not be fully decoded');
  console.error('    → Check DECODED_FILTERS in Key-Value Store for details');
  await Actor.exit();
}

// ══════════════════════════════════════════════════════════════════════════════
//  STEP 4 — DEEP SCRAPE COMPANY PAGES
// ══════════════════════════════════════════════════════════════════════════════

console.log('\n🏢  STEP 4 — Scraping company pages…');

// Build a summary of the filter context to attach to every record
const filterContext = {
  keywords:         filters.keywords,
  headcounts:       filters.headcounts.map(h => h.label),
  headcountGrowth:  filters.headcountGrowth.map(h => h.label),
  industries:       filters.industries.map(i => i.text),
  regions:          filters.regions.map(r => r.text),
  companyTypes:     filters.companyTypes.map(c => c.label),
  revenue: filters.revenue.min !== null
    ? `${filters.revenue.min}M–${filters.revenue.max !== null ? filters.revenue.max + 'M' : '∞'} ${filters.revenue.currency}`
    : null,
  fortune:          filters.fortune,
  technologiesUsed: filters.technologiesUsed.map(t => t.text),
  spotlight:        filters.spotlight.map(s => s.text),
  departmentFilters: filters.departmentHeadcount.length
    ? filters.departmentHeadcount
    : undefined,
  metaOnlyFilters,
};

for (let ci = 0; ci < companyUrls.length; ci++) {
  const companyUrl = companyUrls[ci];
  const slug = companyUrl.match(/\/company\/([^/]+)/)?.[1] || companyUrl;

  console.log(`\n  [${ci + 1}/${companyUrls.length}] ${slug}`);

  if (!deepScrape) {
    // Shallow mode: record the URL we found, don't fetch the profile
    await Actor.pushData({
      slug,
      linkedinUrl:    `https://www.linkedin.com/company/${slug}/`,
      scrapeMode:     'shallow',
      filterContext,
      scrapedAt:      new Date().toISOString(),
    });
    continue;
  }

  const proxy = await getProxyUrl(useApifyProxy, proxyCountryCode, slug.slice(0, 10));
  const { html, ok, status } = await fetchPage(companyUrl, proxy);

  if (!html) {
    console.error(`    ❌ Fetch failed for ${slug}`);
    await Actor.pushData({
      slug,
      linkedinUrl: companyUrl,
      error:       'Fetch failed after all retries',
      httpStatus:  status,
      filterContext,
      scrapedAt:   new Date().toISOString(),
    });
    continue;
  }

  if (detectAuthWall(html)) {
    console.warn(`    ⚠ Auth wall for ${slug} — try with proxy enabled`);
    await Actor.pushData({
      slug,
      linkedinUrl: companyUrl,
      error:       'Auth wall — change IP / enable Apify Proxy',
      httpStatus:  status,
      filterContext,
      scrapedAt:   new Date().toISOString(),
    });
    await sleep(randInt(requestDelayMs * 2, requestDelayMs * 3));
    continue;
  }

  const jsonLd = parseJsonLd(html);
  const page   = scrapeCompanyAbout(html, companyUrl);

  // ── Build the final merged record ─────────────────────────────────────────
  const record = {
    // Identity
    name:            jsonLd?.name          || page.name,
    tagline:         page.tagline,
    description:     jsonLd?.description   || page.description,

    // Contact
    website:         jsonLd?.url           || page.website,
    email:           jsonLd?.email         || null,
    phone:           jsonLd?.telephone     || null,

    // Location
    headquarters:    page.hq               || jsonLd?.address?.addressLocality || null,
    streetAddress:   jsonLd?.address?.streetAddress   || null,
    city:            jsonLd?.address?.addressLocality  || null,
    stateRegion:     jsonLd?.address?.addressRegion    || null,
    country:         jsonLd?.address?.addressCountry   || null,
    postalCode:      jsonLd?.address?.postalCode        || null,
    fullAddress:     jsonLd?.address
      ? [
          jsonLd.address.streetAddress,
          jsonLd.address.addressLocality,
          jsonLd.address.addressRegion,
          jsonLd.address.postalCode,
          jsonLd.address.addressCountry,
        ].filter(Boolean).join(', ')
      : null,

    // Company details
    industry:        page.industry,
    companyType:     page.companyType,
    founded:         page.founded          || jsonLd?.foundingDate || null,
    employeeCount:   page.employeeCount,
    specialties:     page.specialties,

    // Social proof
    followers:       page.followers,
    associatedMembers: page.associatedMembers,
    logo:            page.logo             || jsonLd?.logo || null,

    // Social links (JSON-LD sameAs = other verified profiles)
    linkedinUrl:     page.linkedinUrl      || companyUrl,
    sameAs:          jsonLd?.sameAs        || [],

    // ── Filter context (what SalesNav filters produced this result) ────────
    filterContext,

    // ── Revenue filter metadata (decoded from your SalesNav URL) ──────────
    // NOTE: This is the revenue range YOU filtered BY — not the company's
    // actual revenue. LinkedIn's revenue data is an estimate anyway.
    revenueFilterApplied: filters.revenue.min !== null
      ? {
          min:      filters.revenue.min,
          max:      filters.revenue.max,
          currency: filters.revenue.currency,
          note:     'Revenue filter decoded from SalesNav URL — applies to the SEARCH, not confirmed company revenue',
        }
      : null,

    // ── Growth filter metadata ─────────────────────────────────────────────
    headcountGrowthFilterApplied: filters.headcountGrowth.length
      ? filters.headcountGrowth.map(h => h.label)
      : null,

    // Meta
    slug,
    httpStatus:  status,
    scrapeMode:  'deep',
    scrapedAt:   new Date().toISOString(),
  };

  const label = `${record.name || '(no name)'} | ${record.industry || 'n/a'} | ${record.employeeCount || 'n/a employees'}`;
  console.log(`    ✅ ${label}`);

  await Actor.pushData(record);

  // Polite delay between profile requests
  if (ci < companyUrls.length - 1) {
    await sleep(randInt(requestDelayMs, requestDelayMs + 2500));
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  DONE
// ══════════════════════════════════════════════════════════════════════════════

const ds = await Actor.openDataset();
const { itemCount } = await ds.getInfo();
console.log(`\n🏁  Done! ${itemCount} company records saved to Dataset.`);
console.log('    Export as JSON / CSV / Excel from Apify Console → Storage → Datasets.');

await Actor.exit();
