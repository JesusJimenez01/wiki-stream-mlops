/* ============================================================
   WIKIPEDIA NEWSPAPER — Frontend SPA
   ============================================================ */
const API = '/api';

/* ── Output encoding ──
   Headlines, summaries and tags are written by an LLM from public Wikipedia
   edits, i.e. untrusted input. Every dynamic value goes through esc() before
   reaching innerHTML, and links are restricted to http(s). */
const HTML_ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

function esc(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, ch => HTML_ESCAPES[ch]);
}

function safeUrl(url) {
    try {
        const parsed = new URL(String(url || ''), window.location.origin);
        return (parsed.protocol === 'http:' || parsed.protocol === 'https:') ? parsed.href : '#';
    } catch (e) {
        return '#';
    }
}

const App = {
    currentTab: 'front-page',
    topics: [],
    topTopic: null,

    /* ── Bootstrap ── */
    async init() {
        this.setDate();
        this.bindTabs();
        await this.loadStats();
        this.navigate('front-page');
    },

    setDate() {
        const el = document.getElementById('date-display');
        const d = new Date();
        const opts = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
        const str = d.toLocaleDateString('en-US', opts);
        el.textContent = str.charAt(0).toUpperCase() + str.slice(1);
    },

    bindTabs() {
        document.getElementById('tab-bar').addEventListener('click', e => {
            const tab = e.target.closest('.tab[data-tab]');
            if (!tab) return;
            e.preventDefault();
            this.navigate(tab.dataset.tab);
        });
    },

    setActiveTab(name) {
        document.querySelectorAll('.tab[data-tab]').forEach(t => {
            t.classList.toggle('active', t.dataset.tab === name);
        });
        this.currentTab = name;
    },

    /* ── Stats / ticker ── */
    async loadStats() {
        try {
            const res = await fetch(API + '/stats');
            const data = await res.json();
            this.topics = data.top_topics || [];

            // Set the dynamic topic tab label to the most popular topic
            if (this.topics.length > 0) {
                this.topTopic = this.topics[0].topic_value || this.topics[0].topic;
                const dynTab = document.getElementById('dynamic-topic-tab');
                if (dynTab) {
                    dynTab.textContent = (this.topics[0].topic || 'TRENDING').toUpperCase() + ' (' + this.topics[0].count + ')';
                }
            }

            const el = document.getElementById('ticker-text');
            el.textContent = data.total_news + ' articles cataloged \u2022 '
                + data.live_news + ' live events \u2022 Inference rate: '
                + (data.inference_success_ratio * 100).toFixed(1) + '%';
        } catch (e) {
            console.error('Stats error', e);
        }
    },

    /* ── Navigation ── */
    async navigate(tab) {
        this.setActiveTab(tab);
        const main = document.getElementById('main-content');
        main.innerHTML = '<div class="loader">Composing typographic plates\u2026</div>';

        try {
            switch (tab) {
                case 'front-page': await this.renderFrontPage(main); break;
                case 'latest':     await this.renderLatest(main); break;
                case 'live':       await this.renderLive(main); break;
                case 'top-topic':  await this.renderTopic(main, this.topTopic || 'property'); break;
                default:           await this.renderFrontPage(main); break;
            }
        } catch (err) {
            main.innerHTML = '<div class="empty-state">Printing error. Please try again in a few moments.</div>';
            console.error(err);
        }
    },

    /* ── Fetch helpers ── */
    async fetchNews(params) {
        const res = await fetch(API + '/news' + (params || ''));
        if (!res.ok) throw new Error(res.statusText);
        return (await res.json()).items || [];
    },

    async fetchStory(storyId) {
        const res = await fetch(API + '/news/' + encodeURIComponent(storyId));
        if (!res.ok) throw new Error(res.statusText);
        return await res.json();
    },

    /* ── Tab renderers ── */
    async renderFrontPage(el) {
        const items = await this.fetchNews('?limit=30');
        if (!items.length) { el.innerHTML = '<div class="empty-state">No news available.</div>'; return; }

        let html = '';
        html += this.buildLead(items[0]);
        html += '<div class="front-grid">';
        for (let i = 1; i < items.length; i++) html += this.buildCard(items[i]);
        html += '</div>';
        el.innerHTML = html;
        this.bindCards(el);
    },

    async renderLatest(el) {
        const items = await this.fetchNews('?limit=20');
        if (!items.length) { el.innerHTML = '<div class="empty-state">No recent dispatches.</div>'; return; }

        let html = '<h2 class="section-title">Latest News</h2>';
        html += '<div class="front-grid">';
        for (const item of items) html += this.buildCard(item);
        html += '</div>';
        el.innerHTML = html;
        this.bindCards(el);
    },

    async renderLive(el) {
        const items = await this.fetchNews('?limit=80');
        const live = items.filter(i => i.is_live_event);
        if (!live.length) {
            el.innerHTML = '<div class="empty-state">No live events at the moment.</div>';
            return;
        }
        let html = '<h2 class="section-title">Live Events</h2>';
        html += '<div class="front-grid">';
        for (const item of live) html += this.buildCard(item, true);
        html += '</div>';
        el.innerHTML = html;
        this.bindCards(el);
    },

    async renderTopic(el, topicTerm) {
        const items = await this.fetchNews('?topic=' + encodeURIComponent(topicTerm) + '&limit=20');
        if (!items.length) {
            el.innerHTML = '<div class="empty-state">No articles in the \u201c' + esc(topicTerm) + '\u201d section.</div>';
            return;
        }
        let html = '<h2 class="section-title">' + esc(topicTerm.charAt(0).toUpperCase() + topicTerm.slice(1)) + '</h2>';
        html += this.buildLead(items[0]);
        if (items.length > 1) {
            html += '<div class="front-grid">';
            for (let i = 1; i < items.length; i++) html += this.buildCard(items[i]);
            html += '</div>';
        }
        el.innerHTML = html;
        this.bindCards(el);
    },

    /* ── Article detail ── */
    async showArticle(storyId) {
        const main = document.getElementById('main-content');
        main.innerHTML = '<div class="loader">Unfolding article\u2026</div>';

        try {
            const story = await this.fetchStory(storyId);
            const dateStr = new Date(story.timestamp).toLocaleDateString('en-US', {
                weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
                hour: '2-digit', minute: '2-digit'
            });
            const tags = (story.tags || []).map(function(t) { return '<span>' + esc(t) + '</span>'; }).join('');
            const live = story.is_live_event ? ' <span class="badge-live">Live</span>' : '';

            main.innerHTML = '<div class="article-page">'
                + '<a href="#" class="article-back" id="back-link">\u2190 Back to front page</a>'
                + '<div class="article-kicker">' + esc(story.topic_label || story.topic_term || 'General') + live + '</div>'
                + '<h1 class="article-title">' + esc(story.headline) + '</h1>'
                + '<div class="article-byline">'
                +   'By <strong>Wikipedia Newsroom</strong> &mdash; ' + esc(dateStr) + '<br>'
                +   'Dispatch: ' + esc(story.domain) + ' &bull; ' + esc(story.topic_event_count) + ' related events'
                + '</div>'
                + '<div class="article-body"><p>' + esc(story.summary) + '</p></div>'
                + '<div class="article-tags">' + tags + '</div>'
                + '<div class="article-source">'
                +   '<a href="' + esc(safeUrl(story.title_url)) + '" target="_blank" rel="noopener noreferrer">Read original dispatch on Wikimedia \u2192</a>'
                + '</div>'
                + '</div>';

            document.getElementById('back-link').addEventListener('click', function(e) {
                e.preventDefault();
                App.navigate(App.currentTab);
            });
        } catch (err) {
            main.innerHTML = '<div class="empty-state">Article not found.</div>';
        }
    },

    /* ── HTML builders ── */
    fmtDate(iso) {
        return new Date(iso).toLocaleDateString('en-US', {
            day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit'
        });
    },

    buildLead(item) {
        const live = item.is_live_event ? '<span class="badge-live">Live</span> ' : '';
        return '<div class="lead-wrapper" data-story="' + esc(item.story_id) + '">'
            + '<div class="lead-topic">' + live + esc(item.topic_label || item.topic_term || 'General') + '</div>'
            + '<h2 class="lead-headline">' + esc(item.headline) + '</h2>'
            + '<div class="lead-summary">' + esc(item.summary) + '</div>'
            + '<div class="lead-meta">' + esc(item.domain) + ' &mdash; ' + esc(this.fmtDate(item.timestamp)) + '</div>'
            + '</div>';
    },

    buildCard(item, showLive) {
        const live = (showLive && item.is_live_event) ? '<span class="badge-live">Live</span> ' : '';
        return '<div class="card" data-story="' + esc(item.story_id) + '">'
            + '<div class="card-topic">' + live + esc(item.topic_label || item.topic_term || 'General') + '</div>'
            + '<div class="card-headline">' + esc(item.headline) + '</div>'
            + '<div class="card-summary">' + esc(item.summary) + '</div>'
            + '<div class="card-meta">' + esc(item.domain) + ' &mdash; ' + esc(this.fmtDate(item.timestamp)) + '</div>'
            + '</div>';
    },

    bindCards(container) {
        container.querySelectorAll('[data-story]').forEach(function(el) {
            el.addEventListener('click', function() {
                App.showArticle(el.dataset.story);
            });
        });
    }
};

document.addEventListener('DOMContentLoaded', function() { App.init(); });
