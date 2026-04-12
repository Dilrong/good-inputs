import { writeFile, mkdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';

const SOURCES = [
  {
    source: '국토정책Brief',
    category: '정책',
    listUrl: 'https://www.krihs.re.kr/krihsLibraryReport/briefList.es?mid=a10103050000&pub_kind=BR_1',
    maxItems: 5,
  },
  {
    source: '국토이슈리포트',
    category: '이슈',
    listUrl: 'https://www.krihs.re.kr/krihsLibraryArticle/articleList.es?mid=a10103080000&pub_kind=9',
    maxItems: 5,
  },
  {
    source: '워킹페이퍼',
    category: '연구',
    listUrl: 'https://www.krihs.re.kr/krihsLibraryReport/briefList.es?mid=a10103090000&pub_kind=WKP',
    maxItems: 4,
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
  };

  return str
    .replace(/&(amp|lt|gt|quot|nbsp|middot);|&#039;/g, match => named[match] || match)
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCharCode(parseInt(n, 16)));
}

function stripTags(str = '') {
  return decodeHtml(str)
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      'user-agent': 'Mozilla/5.0 (compatible; GoodInputsBot/1.0)',
    },
  });

  if (!res.ok) {
    throw new Error(`Fetch failed: ${res.status} ${url}`);
  }

  return await res.text();
}

function extractListItems(html, sourceMeta) {
  const regex = /javascript:viewCntAdd(?:2)?\('[^']+','[^']+','([^']+)'\)[\s\S]*?<strong class="title">\s*([\s\S]*?)\s*<\/strong>[\s\S]*?<strong class="label">저자 <\/strong>([\s\S]*?)<\/p>[\s\S]*?<strong class="label"> 발행일 <\/strong>\s*([0-9.-]+)/g;

  const items = [];
  let match;
  while ((match = regex.exec(html)) !== null) {
    const [, path, rawTitle, rawAuthors, rawDate] = match;
    const url = path.startsWith('/library/') ? `https://library.krihs.re.kr${path}` : path;
    items.push({
      source: sourceMeta.source,
      category: sourceMeta.category,
      title: stripTags(rawTitle),
      authors: stripTags(rawAuthors),
      date: rawDate.replace(/\./g, '-').trim(),
      url,
    });
  }

  return items.slice(0, sourceMeta.maxItems);
}

function extractSummary(html) {
  const introMatch = html.match(/<h5[^>]*>소개글<\/h5>[\s\S]*?<div class="IntroSection[^>]*__content"[^>]*>[\s\S]*?<div>([\s\S]*?)<\/div><\/div><\/div><\/section>/i);
  const raw = introMatch ? introMatch[1] : '';
  let text = stripTags(raw);

  const bulletIndex = text.indexOf('➊');
  if (bulletIndex >= 0) text = text.slice(bulletIndex);

  text = text.replace(/\s+/g, ' ').trim();

  if (!text || /KRIHS홈페이지|통합검색|다국어 입력/.test(text)) {
    return '';
  }

  return text.slice(0, 280);
}

function scoreItem(item) {
  const days = Math.floor((Date.now() - new Date(item.date).getTime()) / 86400000);
  if (days <= 7) return 'new';
  if (days <= 21) return 'fresh';
  return 'stable';
}

async function build() {
  const items = [];

  for (const source of SOURCES) {
    const listHtml = await fetchText(source.listUrl);
    const listItems = extractListItems(listHtml, source);

    for (const item of listItems) {
      try {
        const detailHtml = await fetchText(item.url);
        item.summary = extractSummary(detailHtml) || '원문 소개글 추출 실패. 제목과 출처만 우선 표시한다.';
      } catch (error) {
        item.summary = '원문 소개글 추출 실패. 제목과 출처만 우선 표시한다.';
      }

      item.score = scoreItem(item);
      item.tags = [item.category, item.source, item.score];
      items.push(item);
    }
  }

  items.sort((a, b) => String(b.date).localeCompare(String(a.date)));

  return {
    generatedAt: new Date().toISOString(),
    description: '좋은입력 클린 피드. 허용한 원문 소스만 수집해 제목, 날짜, 저자, 소개글 핵심만 노출한다.',
    items,
  };
}

const outputPath = join(process.cwd(), 'data', 'feed.json');
await mkdir(dirname(outputPath), { recursive: true });
const feed = await build();
await writeFile(outputPath, JSON.stringify(feed, null, 2) + '\n', 'utf8');
console.log(`Wrote ${feed.items.length} items to ${outputPath}`);
