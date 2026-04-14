import { writeFile, mkdir, readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const USER_AGENT = 'Mozilla/5.0 (compatible; GoodInputsBot/2.0)';
const TRANSLATION_CACHE_PATH = join(process.cwd(), 'data', 'translation-cache.json');
const SOURCE_HEALTH_PATH = join(process.cwd(), 'data', 'source-health.json');
const SUMMARY_FALLBACK = '요약 추출 실패. 제목과 원문 링크만 우선 표시한다.';

const SOURCES = [
  {
    kind: 'krihs',
    source: '국토정책Brief',
    category: '정책',
    listUrl: 'https://www.krihs.re.kr/krihsLibraryReport/briefList.es?mid=a10103050000&pub_kind=BR_1',
    maxItems: 5,
  },
  {
    kind: 'krihs',
    source: '국토이슈리포트',
    category: '이슈',
    listUrl: 'https://www.krihs.re.kr/krihsLibraryArticle/articleList.es?mid=a10103080000&pub_kind=9',
    maxItems: 5,
  },
  {
    kind: 'krihs',
    source: '워킹페이퍼',
    category: '연구',
    listUrl: 'https://www.krihs.re.kr/krihsLibraryReport/briefList.es?mid=a10103090000&pub_kind=WKP',
    maxItems: 4,
  },
  {
    kind: 'kdi-focus',
    source: 'KDI FOCUS',
    category: '정책',
    listUrl: 'https://www.kdi.re.kr/research/focusList',
    maxItems: 1,
    insecure: true,
  },
  {
    kind: 'kdi-report',
    source: 'KDI 연구보고서',
    category: '연구',
    listUrl: 'https://www.kdi.re.kr/research/reportList?pub_cd=A2&pub_cd=A3',
    maxItems: 4,
    insecure: true,
  },
  {
    kind: 'kcmi',
    source: '자본시장연구원',
    category: '경제',
    listUrl: 'https://www.kcmi.re.kr/report/json_report_list',
    maxItems: 3,
  },
  {
    kind: 'kiep',
    source: 'KIEP',
    category: '경제',
    listUrl: 'https://www.kiep.go.kr/gallery.es?mid=a10101020000&bid=0002',
    maxItems: 3,
  },
  {
    kind: 'bok',
    source: '한국은행',
    category: '경제',
    listUrl: 'https://www.bok.or.kr/portal/singl/crncyPolicyDrcMtg/listYear.do?mtgSe=A&menuNo=200755',
    maxItems: 3,
  },
  {
    kind: 'rss',
    format: 'rss',
    source: '토스 테크',
    category: '개발',
    listUrl: 'https://toss.tech/rss.xml',
    maxItems: 2,
    extraTags: ['개발', '국내'],
  },
  {
    kind: 'rss',
    format: 'atom',
    source: '네이버 D2',
    category: '개발',
    listUrl: 'https://d2.naver.com/d2.atom',
    maxItems: 2,
    extraTags: ['개발', '국내'],
    excludeTags: ['news'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'GitHub Engineering',
    category: '개발',
    listUrl: 'https://github.blog/engineering/feed/',
    maxItems: 2,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['performance', 'agent', 'architecture', 'optimization', 'security', 'diff', 'migration'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Node.js Blog',
    category: '개발',
    listUrl: 'https://nodejs.org/en/feed/blog.xml',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['security', 'release', 'permission', 'v8', 'npm', 'toolchain'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Django Blog',
    category: '개발',
    listUrl: 'https://www.djangoproject.com/rss/weblog/',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['security', 'release'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Python Insider',
    category: '개발',
    listUrl: 'https://blog.python.org/feeds/posts/default?alt=rss',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['security', 'release', 'cpython', 'pep', 'progress'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'pnpm Blog',
    category: '개발',
    listUrl: 'https://pnpm.io/blog/rss.xml',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['release', 'security', 'approve-builds', 'strict', 'catalog'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'React Blog',
    category: '개발',
    listUrl: 'https://react.dev/rss.xml',
    maxItems: 2,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['security', 'release', 'compiler', 'server'],
  },
  {
    kind: 'rss',
    format: 'atom',
    source: 'Go Blog',
    category: '개발',
    listUrl: 'https://go.dev/blog/feed.atom',
    maxItems: 2,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['type', 'compiler', 'inline', 'release', 'security', 'go 1.'],
  },
  {
    kind: 'rss',
    format: 'atom',
    source: 'Rust Blog',
    category: '개발',
    listUrl: 'https://blog.rust-lang.org/feed.xml',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['build', 'release', 'cargo', 'docs.rs', 'security'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Kubernetes Blog',
    category: '개발',
    listUrl: 'https://kubernetes.io/feed.xml',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['release', 'deprecation', 'kubernetes v', 'security', 'feature gate'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'AWS Developer',
    category: '개발',
    listUrl: 'https://aws.amazon.com/blogs/developer/feed/',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['generally available', 'release', 'sdk', 'smithy', 'security', 'performance'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Meta Engineering',
    category: '개발',
    listUrl: 'https://engineering.fb.com/feed/',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['performance', 'modernized', 'webrtc', 'infrastructure', 'architecture', 'efficiency', 'ai'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Dropbox Tech',
    category: '개발',
    listUrl: 'https://dropbox.tech/feed',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['storage', 'efficiency', 'performance', 'architecture', 'infrastructure'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Slack Engineering',
    category: '개발',
    listUrl: 'https://slack.engineering/feed',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['agentic', 'context', 'performance', 'architecture', 'scalability', 'infrastructure'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'Svelte Blog',
    category: '개발',
    listUrl: 'https://svelte.dev/blog/rss.xml',
    maxItems: 1,
    extraTags: ['개발', '글로벌'],
    includeKeywords: ['release', 'migration', 'compiler', 'sveltekit'],
    excludeKeywords: ["what's new", 'what’s new'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'St. Louis Fed',
    category: '경제',
    listUrl: 'https://www.stlouisfed.org/on-the-economy/feed',
    maxItems: 2,
    extraTags: ['경제', '글로벌', '연준'],
    includeKeywords: ['housing', 'inflation', 'credit', 'debit', 'banking', 'labor', 'productivity', 'interest', 'supply', 'recession'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'NY Fed Liberty Street',
    category: '경제',
    listUrl: 'https://libertystreeteconomics.newyorkfed.org/feed/',
    maxItems: 2,
    extraTags: ['경제', '글로벌', '연준'],
    includeKeywords: ['risk', 'market', 'econom', 'inflation', 'resilience', 'mortgage', 'housing', 'labor'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'FRED Blog',
    category: '경제',
    listUrl: 'https://fredblog.stlouisfed.org/feed/',
    maxItems: 2,
    extraTags: ['경제', '글로벌', '데이터'],
    includeKeywords: ['gdp', 'inflation', 'trade', 'labor', 'employment', 'productivity', 'recession'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'NBER Working Papers',
    category: '경제',
    listUrl: 'https://www.nber.org/rss/new.xml',
    maxItems: 2,
    extraTags: ['경제', '글로벌', '연구'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'ECB Blog',
    category: '경제',
    listUrl: 'https://www.ecb.europa.eu/rss/blog.html',
    maxItems: 2,
    extraTags: ['경제', '글로벌', '중앙은행'],
    includeKeywords: ['monetary', 'inflation', 'price', 'policy', 'wage', 'productivity', 'bank'],
  },
  {
    kind: 'rss',
    format: 'rss',
    source: 'ECB Press',
    category: '경제',
    listUrl: 'https://www.ecb.europa.eu/rss/press.html',
    maxItems: 1,
    extraTags: ['경제', '글로벌', '중앙은행'],
    includeKeywords: ['policy', 'inflation', 'bank', 'market', 'competitiveness', 'interest'],
  },
  {
    kind: 'rss',
    format: 'rss',
    kind: 'mmca',
    source: 'MMCA',
    category: '전시',
    listUrl: 'https://www.mmca.go.kr/exhibitions/AjaxExhibitionList.do?exhFlag=1&searchExhPlaCd=&searchExhCd=&sort=1&pageIndex=1&exhLangCd=01',
    maxItems: 5,
  },
  {
    kind: 'museum',
    source: '국립중앙박물관',
    category: '전시',
    listUrl: 'https://www.museum.go.kr/site/main/home',
    maxItems: 4,
  },
  {
    kind: 'ebs',
    source: 'EBS',
    category: '교양',
    listUrl: 'https://www.ebs.co.kr/main',
    maxItems: 5,
    allowPrograms: [/^다큐프라임/],
  },
];

function decodeHtml(str = '') {
  const named = {
    '&amp;': '&',
    '&lt;': '<',
    '&gt;': '>',
    '&quot;': '"',
    '&#039;': "'",
    '&nbsp;': ' ',
    '&middot;': '·',
    '&ldquo;': '“',
    '&rdquo;': '”',
    '&lsquo;': '‘',
    '&rsquo;': '’',
  };

  return String(str)
    .replace(/&(amp|lt|gt|quot|nbsp|middot|ldquo|rdquo|lsquo|rsquo);|&#039;/g, match => named[match] || match)
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCharCode(parseInt(n, 16)));
}

function stripTags(str = '') {
  return decodeHtml(str)
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<\/li>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/[ \t\f\v]+/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .replace(/\n{2,}/g, '\n')
    .trim();
}

function cleanText(str = '') {
  return stripTags(str).replace(/\s+/g, ' ').trim();
}

function truncateText(str = '', max = 280) {
  const text = cleanText(str);
  if (!text) return '';
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1).trim()}…`;
}

function normalizeDate(str = '') {
  const match = String(str).match(/(20\d{2})[.-](\d{2})[.-](\d{2})/);
  if (match) return `${match[1]}-${match[2]}-${match[3]}`;
  return toIsoDate(str) || String(str).trim();
}

function toIsoDate(value = '') {
  const timestamp = Date.parse(String(value).trim());
  if (!Number.isFinite(timestamp)) return '';
  return new Date(timestamp).toISOString().slice(0, 10);
}

function absoluteUrl(base, path) {
  try {
    return new URL(path, base).toString();
  } catch {
    return path;
  }
}

function uniqueBy(items, keyFn) {
  const seen = new Set();
  return items.filter(item => {
    const key = keyFn(item);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function extractMetaContent(html, attr, name) {
  const regex = new RegExp(`<meta[^>]+${attr}=["']${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}["'][^>]+content=["']([\s\S]*?)["']`, 'i');
  return cleanText((html.match(regex) || [])[1] || '');
}

function parseXmlAttributes(fragment = '') {
  const attrs = {};
  for (const match of String(fragment).matchAll(/([:\w-]+)=["']([\s\S]*?)["']/g)) {
    attrs[match[1]] = decodeHtml(match[2]);
  }
  return attrs;
}

function extractXmlTag(block = '', tagNames = []) {
  const names = Array.isArray(tagNames) ? tagNames : [tagNames];
  for (const tagName of names) {
    const escaped = String(tagName).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const match = String(block).match(new RegExp(`<${escaped}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${escaped}>`, 'i'));
    if (match) return match[1];
  }
  return '';
}

function extractAtomLink(block = '') {
  let fallback = '';
  for (const match of String(block).matchAll(/<link\b([^>]*)\/?>(?:<\/link>)?/gi)) {
    const attrs = parseXmlAttributes(match[1]);
    if (!attrs.href) continue;
    if (!fallback) fallback = attrs.href;
    if (!attrs.rel || attrs.rel === 'alternate') return attrs.href;
  }
  return fallback;
}

function extractRssCategories(block = '') {
  return [...String(block).matchAll(/<category(?:\s[^>]*)?>([\s\S]*?)<\/category>/gi)]
    .map(match => cleanText(match[1]))
    .filter(Boolean);
}

function extractAtomCategories(block = '') {
  const categories = [];
  for (const match of String(block).matchAll(/<category\b([^>]*)\/?>(?:([\s\S]*?)<\/category>)?/gi)) {
    const attrs = parseXmlAttributes(match[1]);
    categories.push(cleanText(attrs.term || match[2] || ''));
  }
  return categories.filter(Boolean);
}

function keywordMatch(text = '', keywords = []) {
  const normalized = String(text).toLowerCase();
  return keywords.some(keyword => normalized.includes(String(keyword).toLowerCase()));
}

function sourceAllowsItem(source, item) {
  const haystack = [item.title, item.summary, item.authors, ...(item.tags || [])].join(' ').toLowerCase();
  if (source.includeKeywords?.length && !keywordMatch(haystack, source.includeKeywords)) return false;
  if (source.excludeKeywords?.length && keywordMatch(haystack, source.excludeKeywords)) return false;
  return true;
}

async function fetchText(url, options = {}) {
  const headers = {
    'user-agent': USER_AGENT,
    ...(options.headers || {}),
  };

  try {
    const res = await fetch(url, {
      method: options.method || 'GET',
      headers,
      body: options.body,
    });
    if (!res.ok) throw new Error(`Fetch failed: ${res.status} ${url}`);
    return await res.text();
  } catch (error) {
    const args = ['-sL', '--compressed', '-A', USER_AGENT, '-X', options.method || 'GET'];
    if (options.insecure) args.push('-k');
    for (const [key, value] of Object.entries(options.headers || {})) {
      args.push('-H', `${key}: ${value}`);
    }
    if (typeof options.body === 'string') {
      args.push('--data', options.body);
    }
    args.push(url);
    const { stdout } = await execFileAsync('curl', args, { maxBuffer: 20 * 1024 * 1024 });
    if (!stdout) throw error;
    return stdout;
  }
}

async function fetchJson(url, options = {}) {
  return JSON.parse(await fetchText(url, options));
}

async function readJsonIfExists(path) {
  try {
    return JSON.parse(await readFile(path, 'utf8'));
  } catch {
    return {};
  }
}

function isMostlyKorean(text = '') {
  return /[가-힣]/.test(String(text));
}

function countMatches(text = '', regex) {
  return (String(text).match(regex) || []).length;
}

function shouldTranslateItem(item) {
  const sample = (item.summary && item.summary !== SUMMARY_FALLBACK ? item.summary : item.title).trim();
  return Boolean(sample) && !isMostlyKorean(sample);
}

async function translateToKorean(text = '') {
  const input = cleanText(text);
  if (!input) return '';

  const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=ko&dt=t&q=${encodeURIComponent(input)}`;
  try {
    const data = JSON.parse(await fetchText(url));
    return cleanText((data?.[0] || []).map(part => part?.[0] || '').join(' '));
  } catch {
    return '';
  }
}

function isUsableKoreanTranslation(sourceText = '', translatedText = '') {
  const source = cleanText(sourceText);
  const translated = cleanText(translatedText);
  if (!source || !translated) return false;
  if (source === translated) return false;
  if (!/[가-힣]/.test(translated)) return false;
  if (/^발췌(?:\s|$)/.test(translated)) return false;

  const hangulCount = countMatches(translated, /[가-힣]/g);
  const latinCount = countMatches(translated, /[A-Za-z]/g);
  const compactLength = translated.replace(/\s+/g, '').length || 1;

  if (hangulCount < 4) return false;
  if (source.length >= 60 && translated.length < 18) return false;
  if ((hangulCount / compactLength) < 0.2) return false;
  if (latinCount > hangulCount * 1.5 && translated.length > 24) return false;

  return true;
}

function readCachedTranslation(cacheEntry, sourceText = '') {
  if (!cacheEntry || cacheEntry.input !== sourceText) return null;
  const candidate = cleanText(cacheEntry.koText || cacheEntry.koSummary || '');
  if (typeof cacheEntry.approved === 'boolean') {
    return cacheEntry.approved && isUsableKoreanTranslation(sourceText, candidate) ? candidate : '';
  }
  return isUsableKoreanTranslation(sourceText, candidate) ? candidate : '';
}

async function translateWithGuard(sourceText, cacheKey, cache) {
  const cached = readCachedTranslation(cache[cacheKey], sourceText);
  if (cached !== null) return cached;

  const raw = cleanText(await translateToKorean(sourceText));
  const approved = isUsableKoreanTranslation(sourceText, raw);
  const koText = approved ? truncateText(raw, 220) : '';

  cache[cacheKey] = {
    input: sourceText,
    approved,
    koText,
  };

  return koText;
}

function scoreItem(item) {
  const timestamp = new Date(item.date).getTime();
  if (!Number.isFinite(timestamp)) return 'stable';
  const days = Math.floor((Date.now() - timestamp) / 86400000);
  if (days <= 7) return 'new';
  if (days <= 21) return 'fresh';
  return 'stable';
}

function finalizeItem(item, extraTags = []) {
  const next = {
    ...item,
    title: cleanText(item.title),
    authors: cleanText(item.authors || ''),
    date: normalizeDate(item.date || ''),
    summary: truncateText(item.summary || ''),
    url: item.url,
  };

  next.score = scoreItem(next);
  next.tags = uniqueBy(
    [next.category, next.source, ...extraTags, next.score].filter(Boolean).map(tag => cleanText(tag)),
    tag => tag,
  );

  if (!next.summary) {
    next.summary = SUMMARY_FALLBACK;
  }

  return next;
}

function extractRssFeedItems(xml, source) {
  const items = [];

  for (const match of String(xml).matchAll(/<item\b[\s\S]*?<\/item>/gi)) {
    const block = match[0];
    const categories = extractRssCategories(block);
    if (source.excludeTags?.some(tag => categories.includes(tag))) continue;

    const item = finalizeItem({
      source: source.source,
      category: source.category,
      title: extractXmlTag(block, 'title'),
      authors: extractXmlTag(block, ['dc:creator', 'author']),
      date: normalizeDate(extractXmlTag(block, ['pubDate', 'dc:date'])),
      url: extractXmlTag(block, 'link'),
      summary: extractXmlTag(block, ['description', 'content:encoded']),
    }, [...(source.extraTags || []), ...categories]);

    if (!sourceAllowsItem(source, item)) continue;
    items.push(item);
  }

  return uniqueBy(items, item => item.url).slice(0, source.maxItems);
}

function extractAtomFeedItems(xml, source) {
  const items = [];

  for (const match of String(xml).matchAll(/<entry\b[\s\S]*?<\/entry>/gi)) {
    const block = match[0];
    const categories = extractAtomCategories(block);
    if (source.excludeTags?.some(tag => categories.includes(tag))) continue;

    const item = finalizeItem({
      source: source.source,
      category: source.category,
      title: extractXmlTag(block, 'title'),
      authors: extractXmlTag(block, ['name', 'author']),
      date: normalizeDate(extractXmlTag(block, ['published', 'updated'])),
      url: extractAtomLink(block),
      summary: extractXmlTag(block, ['summary', 'content']),
    }, [...(source.extraTags || []), ...categories]);

    if (!sourceAllowsItem(source, item)) continue;
    items.push(item);
  }

  return uniqueBy(items, item => item.url).slice(0, source.maxItems);
}

async function buildRssSource(source) {
  const xml = await fetchText(source.listUrl, { insecure: source.insecure });
  const format = source.format || (/\<feed\b/i.test(xml) ? 'atom' : 'rss');
  return format === 'atom'
    ? extractAtomFeedItems(xml, source)
    : extractRssFeedItems(xml, source);
}

function extractKrihsListItems(html, sourceMeta) {
  const regex = /javascript:viewCntAdd(?:2)?\('[^']+','[^']+','([^']+)'\)[\s\S]*?<strong class="title">\s*([\s\S]*?)\s*<\/strong>[\s\S]*?<strong class="label">저자 <\/strong>([\s\S]*?)<\/p>[\s\S]*?<strong class="label"> 발행일 <\/strong>\s*([0-9.-]+)/g;
  const items = [];
  let match;

  while ((match = regex.exec(html)) !== null) {
    const [, path, rawTitle, rawAuthors, rawDate] = match;
    items.push({
      source: sourceMeta.source,
      category: sourceMeta.category,
      title: rawTitle,
      authors: rawAuthors,
      date: rawDate,
      url: path.startsWith('/library/') ? `https://library.krihs.re.kr${path}` : path,
    });
  }

  return items.slice(0, sourceMeta.maxItems);
}

function extractKrihsSummary(html) {
  const introMatch = html.match(/<h5[^>]*>소개글<\/h5>[\s\S]*?<div class="IntroSection[^>]*__content"[^>]*>[\s\S]*?<div>([\s\S]*?)<\/div><\/div><\/div><\/section>/i);
  let text = cleanText(introMatch ? introMatch[1] : '');

  const bulletIndex = text.indexOf('➊');
  if (bulletIndex >= 0) text = text.slice(bulletIndex);
  if (!text || /KRIHS홈페이지|통합검색|다국어 입력/.test(text)) return '';
  return text;
}

async function buildKrihsSource(source) {
  const listHtml = await fetchText(source.listUrl);
  const listItems = extractKrihsListItems(listHtml, source);
  const items = [];

  for (const item of listItems) {
    try {
      const detailHtml = await fetchText(item.url);
      item.summary = extractKrihsSummary(detailHtml);
    } catch {
      item.summary = '';
    }
    items.push(finalizeItem(item));
  }

  return items;
}

function extractKdiFocusItems(html, source) {
  const regex = /<a href="([^\"]*\/research\/focusView\?pub_no=\d+)"[^>]*>[\s\S]*?<b class="[^"]*">([\s\S]*?)<\/b>[\s\S]*?<strong>([\s\S]*?)<\/strong>[\s\S]*?<p>([\s\S]*?)<\/p>[\s\S]*?<span>(20\d{2}\.\d{2}\.\d{2})<\/span>/g;
  const items = [];
  let match;

  while ((match = regex.exec(html)) !== null) {
    const [, href, label, title, summary, date] = match;
    items.push(finalizeItem({
      source: source.source,
      category: source.category,
      title,
      authors: '',
      date,
      url: absoluteUrl('https://www.kdi.re.kr', href),
      summary,
    }, [label]));
  }

  return uniqueBy(items, item => item.url).slice(0, source.maxItems);
}

function extractKdiReportListItems(html, source) {
  const regex = /<li>\s*<a href="(\.\/reportView\?pub_no=\d+)">[\s\S]*?<div class="rpt_tit">[\s\S]*?<b class="[^"]*">([\s\S]*?)<\/b>[\s\S]*?<strong>([\s\S]*?)<\/strong>[\s\S]*?<div class="rpt_other">[\s\S]*?<p>([\s\S]*?)<\/p>[\s\S]*?<div class="rpt_link">/g;
  const items = [];
  let match;

  while ((match = regex.exec(html)) !== null) {
    const [, href, label, title, metaBlock] = match;
    const spans = [...metaBlock.matchAll(/<span>([\s\S]*?)<\/span>/g)].map(part => cleanText(part[1])).filter(Boolean);
    items.push({
      source: source.source,
      category: source.category,
      title,
      authors: spans[0] || '',
      date: '',
      url: absoluteUrl('https://www.kdi.re.kr/research/', href),
      summary: '',
      label: cleanText(label),
    });
  }

  return uniqueBy(items, item => item.url).slice(0, source.maxItems);
}

function extractKdiReportDetail(html) {
  const summary = cleanText(
    (html.match(/국문요약[\s\S]*?<div[^>]*class="editor-template"[^>]*>([\s\S]*?)<\/div>/i) || [])[1] ||
    extractMetaContent(html, 'name', 'description') ||
    extractMetaContent(html, 'property', 'og:description')
  );
  const date = normalizeDate((html.match(/view_fixed-info[\s\S]*?<span>(20\d{2}\.\d{2}\.\d{2})<\/span>/i) || [])[1] || '');
  return { summary, date };
}

async function buildKdiReportSource(source) {
  const listHtml = await fetchText(source.listUrl, { insecure: source.insecure });
  const baseItems = extractKdiReportListItems(listHtml, source);
  const items = [];

  for (const item of baseItems) {
    try {
      const detailHtml = await fetchText(item.url, { insecure: source.insecure });
      const detail = extractKdiReportDetail(detailHtml);
      item.summary = detail.summary;
      item.date = detail.date;
    } catch {
      item.summary = item.summary || '';
    }

    items.push(finalizeItem(item, [item.label]));
  }

  return items;
}

async function buildKcmiSource(source) {
  const body = new URLSearchParams({
    thispage: '1',
    perpage: String(source.maxItems),
    s_report_subject: '',
    s_report_type: '',
  });
  const text = await fetchText(source.listUrl, {
    headers: {
      'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    },
    method: 'POST',
    body: body.toString(),
  });
  const data = JSON.parse(text);
  return (Array.isArray(data) ? data : []).slice(0, source.maxItems).map(item => finalizeItem({
    source: source.source,
    category: source.category,
    title: item.report_title,
    authors: item.author,
    date: item.pub_date,
    url: absoluteUrl('https://www.kcmi.re.kr', item.report_pdf_preview_link || item.report_pdf_download_link || ''),
    summary: item.summary,
  }, ['경제', '국내', item.report_type_name, item.report_subject_name]));
}

function extractKiepListItems(html, source) {
  const block = (html.match(/<div class="board_list">([\s\S]*?)<div class="board_pager">/i) || [])[1] || '';
  const items = [];

  for (const match of block.matchAll(/<li>([\s\S]*?)<\/li>/gi)) {
    const row = match[1];
    const href = (row.match(/<a href="([^"]+)"[^>]*class="title"/i) || [])[1] || '';
    const title = (row.match(/class="title">\s*([\s\S]*?)\s*<\/a>/i) || [])[1] || '';
    const info = cleanText((row.match(/<p class="info">([\s\S]*?)<\/p>/i) || [])[1] || '');
    const author = (info.match(/^(.+?)\s+작성일/) || [])[1] || '';
    const date = (info.match(/작성일\s*([0-9.]+)/) || [])[1] || '';
    if (!href || !title) continue;

    items.push({
      source: source.source,
      category: source.category,
      title,
      authors: author,
      date,
      url: absoluteUrl('https://www.kiep.go.kr', href),
      summary: '대외경제, 공급망, 통상, 성장구조 관련 연구보고서.',
    });
  }

  return items.slice(0, source.maxItems);
}

async function buildKiepSource(source) {
  const html = await fetchText(source.listUrl);
  return extractKiepListItems(html, source).map(item => finalizeItem(item, ['경제', '국내']));
}

function extractBokMeetingItems(html, source) {
  const body = (html.match(/<tbody>([\s\S]*?)<\/tbody>/i) || [])[1] || '';
  const currentYear = new Date().getUTCFullYear();
  const items = [];

  for (const match of body.matchAll(/<tr>([\s\S]*?)<\/tr>/gi)) {
    const row = match[1];
    const dayText = cleanText((row.match(/<th[^>]*>([\s\S]*?)<\/th>/i) || [])[1] || '');
    const monthDay = dayText.match(/(\d{2})월\s*(\d{2})일/);
    const viewHref = (row.match(/<a href='([^']+B0000169\/view\.do[^']+)'/i) || [])[1] || '';
    const title = cleanText((row.match(/class="i-goView">([\s\S]*?)<\/a>/i) || [])[1] || '');
    if (!monthDay || !viewHref || !title) continue;

    items.push({
      source: source.source,
      category: source.category,
      title,
      authors: '',
      date: `${currentYear}-${monthDay[1]}-${monthDay[2]}`,
      url: absoluteUrl('https://www.bok.or.kr', decodeHtml(viewHref)),
      summary: '통화정책방향 결정회의와 관련된 총재 기자간담회 및 회의 자료.',
    });
  }

  return items.slice(0, source.maxItems);
}

async function buildBokSource(source) {
  const html = await fetchText(source.listUrl);
  return extractBokMeetingItems(html, source).map(item => finalizeItem(item, ['경제', '국내', '중앙은행']));
}

async function buildMmcaSource(source) {
  const data = await fetchJson(source.listUrl, {
    headers: { 'x-requested-with': 'XMLHttpRequest' },
  });

  const list = Array.isArray(data.exhibitionsList) ? data.exhibitionsList : [];
  return list.slice(0, source.maxItems).map(exhibition => finalizeItem({
    source: source.source,
    category: source.category,
    title: exhibition.exhTitle,
    authors: exhibition.exhArtist || '',
    date: exhibition.exhStDt,
    url: `https://www.mmca.go.kr/exhibitions/exhibitionsDetail.do?exhFlag=1&exhId=${exhibition.exhId}`,
    summary: exhibition.exhContents || `${exhibition.exhPlaNm} ${exhibition.exhPlaDtl}`,
  }, [exhibition.exhPlaNm, '현재전시']));
}

function extractMuseumItems(html, source) {
  const regex = /<div class="swiper-slide">[\s\S]*?<a href="([^"]+)"[^>]*>[\s\S]*?<div class="over-cont">\s*<span>(.*?)<\/span>[\s\S]*?<div class="txt">[\s\S]*?<span class="label-type black">현재전시<\/span>[\s\S]*?<span class="label-type black-4">(.*?)<\/span>[\s\S]*?<a href="[^"]+"[^>]*>(.*?)<\/a>[\s\S]*?<p class="date">(.*?)<\/p>/g;
  const items = [];
  let match;

  while ((match = regex.exec(html)) !== null) {
    const [, href, venue, type, title, date] = match;
    if (cleanText(venue) !== '국립중앙박물관') continue;
    items.push({
      source: source.source,
      category: source.category,
      title,
      authors: '',
      date: normalizeDate(String(date).split('~')[0] || ''),
      url: absoluteUrl('https://www.museum.go.kr', href),
      summary: '',
      type: cleanText(type),
    });
  }

  return uniqueBy(items, item => item.url).slice(0, source.maxItems);
}

function extractMuseumSummary(html) {
  const shortSummary = cleanText((html.match(/<strong>전시요약<\/strong>\s*<p>([\s\S]*?)<\/p>/i) || [])[1] || '');
  if (shortSummary) return shortSummary;

  const content = (html.match(/<!--내용-->[\s\S]*?<div class="view-info-cont">([\s\S]*?)<div class="btn-list">/i) || [])[1] || '';
  const paragraphs = [...content.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)]
    .map(match => cleanText(match[1]))
    .filter(text => text && !/^ㅇ/.test(text) && text.length > 20);

  return paragraphs[0] || '';
}

async function buildMuseumSource(source) {
  const homeHtml = await fetchText(source.listUrl);
  const baseItems = extractMuseumItems(homeHtml, source);
  const items = [];

  for (const item of baseItems) {
    try {
      const detailHtml = await fetchText(item.url);
      item.summary = extractMuseumSummary(detailHtml);
    } catch {
      item.summary = '';
    }

    items.push(finalizeItem(item, [item.type, '현재전시']));
  }

  return items;
}

function extractEbsItems(html, source) {
  const blockRegex = /<a href="(https:[^"]*\/tv\/show\?prodId=\d+&lectId=\d+&new)"[\s\S]*?<\/a>/g;
  const items = [];

  for (const match of html.matchAll(blockRegex)) {
    const url = match[1];
    const block = match[0];
    const episodeTitle = cleanText((block.match(/<span class="cnt">([\s\S]*?)<\/span>/) || [])[1] || '');
    const program = cleanText((block.match(/<span class="item">([\s\S]*?)<\/span>/) || [])[1] || '');

    if (!program || !episodeTitle) continue;
    if (source.allowPrograms?.length && !source.allowPrograms.some(pattern => pattern.test(program))) continue;

    items.push({
      source: source.source,
      category: source.category,
      title: `${program} | ${episodeTitle}`,
      authors: program,
      date: '',
      url,
      summary: '',
      program,
    });
  }

  return uniqueBy(items, item => item.url).slice(0, source.maxItems);
}

function extractEbsDetail(html) {
  const date = normalizeDate((html.match(/"uploadDate"\s*:\s*"(20\d{2}-\d{2}-\d{2})T/i) || [])[1] || '');
  const summary = cleanText(
    extractMetaContent(html, 'property', 'og:description') ||
    (html.match(/"description"\s*:\s*"([\s\S]*?)"/i) || [])[1] || ''
  );
  return { date, summary };
}

async function buildEbsSource(source) {
  const homeHtml = await fetchText(source.listUrl);
  const baseItems = extractEbsItems(homeHtml, source);
  const items = [];

  for (const item of baseItems) {
    try {
      const detailHtml = await fetchText(item.url);
      const detail = extractEbsDetail(detailHtml);
      item.date = detail.date;
      item.summary = detail.summary;
    } catch {
      item.summary = item.summary || '';
    }

    items.push(finalizeItem(item, [item.program]));
  }

  return items;
}

async function buildSource(source) {
  switch (source.kind) {
    case 'krihs':
      return await buildKrihsSource(source);
    case 'kdi-focus': {
      const html = await fetchText(source.listUrl, { insecure: source.insecure });
      return extractKdiFocusItems(html, source);
    }
    case 'kdi-report':
      return await buildKdiReportSource(source);
    case 'kcmi':
      return await buildKcmiSource(source);
    case 'kiep':
      return await buildKiepSource(source);
    case 'bok':
      return await buildBokSource(source);
    case 'rss':
      return await buildRssSource(source);
    case 'mmca':
      return await buildMmcaSource(source);
    case 'museum':
      return await buildMuseumSource(source);
    case 'ebs':
      return await buildEbsSource(source);
    default:
      return [];
  }
}

async function addKoreanSummaries(items) {
  const cache = await readJsonIfExists(TRANSLATION_CACHE_PATH);
  const stats = { attempted: 0, kept: 0, rejected: 0, titleFallback: 0 };

  for (const item of items) {
    if (!shouldTranslateItem(item)) continue;
    stats.attempted += 1;

    const summaryText = item.summary && item.summary !== SUMMARY_FALLBACK
      ? item.summary
      : '';
    const titleText = item.title;
    const baseKey = item.url || `${item.source}|${item.title}`;

    let koSummary = '';
    if (summaryText) {
      koSummary = await translateWithGuard(summaryText, `${baseKey}|summary`, cache);
    }

    if (!koSummary && titleText) {
      koSummary = await translateWithGuard(titleText, `${baseKey}|title`, cache);
      if (koSummary) stats.titleFallback += 1;
    }

    if (!koSummary) {
      delete item.koSummary;
      stats.rejected += 1;
      continue;
    }

    item.koSummary = koSummary;
    stats.kept += 1;
  }

  await writeFile(TRANSLATION_CACHE_PATH, JSON.stringify(cache, null, 2) + '\n', 'utf8');
  return { items, stats };
}

function assessSourceHealth(source, itemCount, previousEntry = {}) {
  const previousCount = Number(previousEntry.itemCount || 0);
  let status = 'ok';
  let detail = 'stable';

  if (itemCount === 0) {
    status = 'warn';
    detail = 'empty';
  } else if (previousCount >= 2 && itemCount < Math.ceil(previousCount / 2)) {
    status = 'warn';
    detail = 'drop';
  }

  return {
    source: source.source,
    category: source.category,
    maxItems: source.maxItems,
    itemCount,
    previousCount,
    status,
    detail,
    checkedAt: new Date().toISOString(),
  };
}

async function writeSourceHealth(entries, translationStats) {
  const summary = entries.reduce((acc, entry) => {
    acc.total += 1;
    acc[entry.status] = (acc[entry.status] || 0) + 1;
    return acc;
  }, { total: 0, ok: 0, warn: 0, error: 0 });

  await writeFile(SOURCE_HEALTH_PATH, JSON.stringify({
    generatedAt: new Date().toISOString(),
    summary,
    translation: translationStats,
    sources: entries,
  }, null, 2) + '\n', 'utf8');
}

async function build() {
  const items = [];
  const previousHealth = await readJsonIfExists(SOURCE_HEALTH_PATH);
  const previousMap = Object.fromEntries((previousHealth.sources || []).map(entry => [entry.source, entry]));
  const sourceHealth = [];

  for (const source of SOURCES) {
    try {
      const nextItems = await buildSource(source);
      items.push(...nextItems);
      sourceHealth.push(assessSourceHealth(source, nextItems.length, previousMap[source.source]));
      console.log(`Loaded ${nextItems.length} items from ${source.source}`);
    } catch (error) {
      sourceHealth.push({
        source: source.source,
        category: source.category,
        maxItems: source.maxItems,
        itemCount: 0,
        previousCount: Number(previousMap[source.source]?.itemCount || 0),
        status: 'error',
        detail: 'fetch-failed',
        error: error.message,
        checkedAt: new Date().toISOString(),
      });
      console.warn(`Skipped ${source.source}: ${error.message}`);
    }
  }

  const uniqueItems = uniqueBy(items, item => item.url).sort((a, b) => String(b.date).localeCompare(String(a.date)));

  if (!uniqueItems.length) {
    throw new Error('No feed items collected.');
  }

  const { stats: translationStats } = await addKoreanSummaries(uniqueItems);
  await writeSourceHealth(sourceHealth, translationStats);

  for (const entry of sourceHealth.filter(entry => entry.status !== 'ok')) {
    console.warn(`Health ${entry.status} ${entry.source}: ${entry.detail}${entry.error ? ` (${entry.error})` : ''}`);
  }
  console.log(`Translation kept ${translationStats.kept}/${translationStats.attempted}, rejected ${translationStats.rejected}, title fallback ${translationStats.titleFallback}`);

  return {
    generatedAt: new Date().toISOString(),
    description: '좋은입력 클린 피드. 정책, 연구, 개발, 전시 원문을 수집해 제목, 날짜, 핵심 요약만 시간순으로 노출한다.',
    items: uniqueItems,
  };
}

const outputPath = join(process.cwd(), 'data', 'feed.json');
await mkdir(dirname(outputPath), { recursive: true });
const feed = await build();
await writeFile(outputPath, JSON.stringify(feed, null, 2) + '\n', 'utf8');
console.log(`Wrote ${feed.items.length} items to ${outputPath}`);
