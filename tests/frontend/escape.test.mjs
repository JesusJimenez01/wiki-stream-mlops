// Frontend output encoding: run with `node --test tests/frontend/*.test.mjs`.
// app.js is a browser script, so it is evaluated in a minimal sandbox.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../newsroom-api/static/app.js', import.meta.url), 'utf8');
const sandbox = {
    window: { location: { origin: 'http://localhost:8085' } },
    document: { addEventListener() {} },
    URL,
};
vm.createContext(sandbox);
vm.runInContext(source + '\n;globalThis.__exports = { esc, safeUrl, App };', sandbox);
const { esc, safeUrl, App } = sandbox.__exports;

test('esc neutralises HTML metacharacters', () => {
    assert.equal(esc('<img src=x onerror="alert(1)">'), '&lt;img src=x onerror=&quot;alert(1)&quot;&gt;');
    assert.equal(esc("Tom & Jerry's"), 'Tom &amp; Jerry&#39;s');
    assert.equal(esc(null), '');
    assert.equal(esc(42), '42');
});

test('safeUrl only allows http(s) links', () => {
    assert.equal(safeUrl('https://en.wikipedia.org/wiki/Artemis_II'), 'https://en.wikipedia.org/wiki/Artemis_II');
    assert.equal(safeUrl('javascript:alert(1)'), '#');
    assert.equal(safeUrl('data:text/html,<script>alert(1)</script>'), '#');
    assert.equal(safeUrl(undefined), 'http://localhost:8085/');
});

test('cards never render LLM output as markup', () => {
    const item = {
        story_id: 'x" onmouseover="alert(1)',
        topic_label: '<b>Space</b>',
        headline: '<script>alert("xss")</script>',
        summary: '<img src=x onerror=alert(1)>',
        domain: 'en.wikipedia.org',
        timestamp: '2026-09-28T12:00:00Z',
    };
    for (const html of [App.buildCard(item), App.buildLead(item)]) {
        assert.ok(!html.includes('<script>'));
        assert.ok(!html.includes('<img'));
        assert.ok(!html.includes('<b>'));
        assert.ok(!html.includes('" onmouseover="'));
        assert.ok(html.includes('&lt;script&gt;'));
    }
});
