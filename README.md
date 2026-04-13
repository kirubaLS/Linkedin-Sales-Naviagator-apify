# LinkedIn SalesNav → Public Company Scraper

**Apify Actor | No Login | No Cookies | Educational Use Only**

---

## What This Does

Paste your LinkedIn Sales Navigator company/account search URL (with all your filters applied) as input. The actor:

1. **Decodes** every filter type baked into your SalesNav URL
2. **Rebuilds** an equivalent query against LinkedIn's public search
3. **Scrapes** each matching company's public `/about/` page
4. **Outputs** rich structured company data to the Apify Dataset

---

## Decoded Filter Types

| Filter | Decoded | Mapped to Public Search |
|--------|---------|------------------------|
| `COMPANY_HEADCOUNT` | ✅ Full (A–I buckets) | ✅ companySize param |
| `INDUSTRY` | ✅ 144 industry IDs | ✅ industry param |
| `REGION` / `GEOGRAPHY` | ✅ All geo IDs + text | ✅ geoUrn param |
| `ANNUAL_REVENUE` | ✅ min/max + currency | ⚠ Metadata only* |
| `COMPANY_TYPE` | ✅ All 8 types | ⚠ Metadata only* |
| `HEADCOUNT_GROWTH` | ✅ All growth bands | ⚠ Metadata only* |
| `FORTUNE` | ✅ Fortune 10/50/100/500 | ⚠ Metadata only* |
| `FOLLOWERS_OF` | ✅ All follower buckets | ⚠ Metadata only* |
| `TECHNOLOGIES_USED` | ✅ ID + text | ⚠ Metadata only* |
| `SPOTLIGHT` | ✅ All spotlight types | ⚠ Metadata only* |
| `DEPARTMENT_HEADCOUNT` | ✅ Dept + range | ⚠ Metadata only* |
| `DEPARTMENT_HEADCOUNT_GROWTH` | ✅ Dept + growth % | ⚠ Metadata only* |
| `CURRENT_COMPANY` | ✅ ID + name | ⚠ Metadata only* |
| `PAST_COMPANY` | ✅ ID + name | ⚠ Metadata only* |
| keywords | ✅ | ✅ keywords param |
| `POSTAL_CODE` | ✅ | ⚠ Metadata only* |

> **\* Metadata only** means the filter is decoded and attached to every output record
> as `filterContext`, but LinkedIn's **public** search has no equivalent parameter.
> The public search is narrowed by the mappable filters (headcount, industry, region,
> keywords) — the rest are surfaced as context.

---

## Output Fields Per Company

```json
{
  "name": "Infosys",
  "tagline": "Navigate your next",
  "description": "Infosys is a global leader in...",
  "website": "https://www.infosys.com",
  "email": null,
  "phone": null,
  "headquarters": "Bengaluru, Karnataka, India",
  "city": "Bengaluru",
  "stateRegion": "Karnataka",
  "country": "India",
  "postalCode": "560100",
  "fullAddress": "Electronics City, Bengaluru, Karnataka, 560100, India",
  "industry": "IT Services and IT Consulting",
  "companyType": "Public Company",
  "founded": "1981",
  "employeeCount": "10,001+ employees",
  "specialties": ["AI", "cloud", "digital transformation", "consulting"],
  "followers": "4.2M followers",
  "associatedMembers": "342,000+ associated members",
  "logo": "https://media.licdn.com/dms/...",
  "linkedinUrl": "https://www.linkedin.com/company/infosys/",
  "sameAs": ["https://twitter.com/Infosys", "https://en.wikipedia.org/wiki/Infosys"],
  "filterContext": {
    "keywords": "software",
    "headcounts": ["51-200"],
    "industries": ["IT Services and IT Consulting"],
    "regions": ["India"],
    "revenue": "1M–50M USD",
    "technologiesUsed": ["Salesforce", "AWS"],
    "metaOnlyFilters": ["ANNUAL_REVENUE", "TECHNOLOGIES_USED"]
  },
  "revenueFilterApplied": {
    "min": 1,
    "max": 50,
    "currency": "USD",
    "note": "Revenue filter decoded from SalesNav URL — applies to the SEARCH, not confirmed company revenue"
  },
  "slug": "infosys",
  "httpStatus": 200,
  "scrapeMode": "deep",
  "scrapedAt": "2025-04-13T10:00:00.000Z"
}
```

---

## Deploy

### Option A — Apify CLI
```bash
npm install -g apify-cli
apify login
cd linkedin-salenav-company-scraper
apify push
```

### Option B — Apify Console (no CLI)
1. Go to **apify.com → Actors → Create New**
2. Select **Multi-file Editor**
3. Create the exact folder structure and paste each file
4. Click **Build → Run**

---

## File Structure
```
linkedin-salenav-company-scraper/
├── .actor/
│   ├── actor.json
│   └── input_schema.json
├── src/
│   └── main.js
├── package.json
├── Dockerfile
└── README.md
```

---

## Important Notes

- **Educational use only.** LinkedIn's ToS prohibits automated scraping.
- The actor only accesses publicly visible data (same as any anonymous browser).
- Sales Navigator itself requires a paid login — this actor does NOT access it.
  It decodes your SalesNav URL's filter parameters and searches LinkedIn's public equivalent.
- Revenue, growth rate, technologies, and other advanced SalesNav filters are decoded
  and recorded as metadata but cannot be applied to public search results.
- Enable Apify Proxy for best success rates. LinkedIn blocks datacenter IPs aggressively.
