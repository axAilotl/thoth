// Exercise the shipped panel event handlers with Node's built-in test runner.
// The small DOM substitute rejects payload HTML writes; no browser packages needed.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const staticRoot = path.join(__dirname, '..', 'static');
const template = fs.readFileSync(path.join(staticRoot, 'review-panel.html'), 'utf8');
const script = fs.readFileSync(path.join(staticRoot, 'review.js'), 'utf8');

class Element {
    constructor(tag = 'div', id = '') {
        this.tagName = tag;
        this.id = id;
        this.children = [];
        this.listeners = {};
        this.attributes = {};
        this.disabled = this.checked = this.hidden = false;
        this.modalCount = 0;
        this.classList = {contains: () => false};
        this._text = '';
    }
    set textContent(value) { this._text = String(value); this.children = []; }
    get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
    set innerHTML(value) {
        assert.equal(this.id, 'review', 'Only the fixed panel template may use HTML');
        assert.equal(value, template);
    }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) { this._text = ''; this.children = children; }
    setAttribute(name, value) { this.attributes[name] = value; }
    addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
    showModal() { this.open = true; this.modalCount += 1; }
    close() { this.open = false; }
    focus() { this.focused = true; }
    async fire(type) {
        const event = {defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }};
        for (const handler of this.listeners[type] || []) await handler(event);
        return event;
    }
}

function item(id, overrides = {}) {
    return {artifact_id: id, revision: id.repeat(64), title: `Paper ${id}`,
        status: 'needs_review', source: 'web_clipper', attempts: 1,
        reason: 'Security review required', reason_summary: 'Security flag: quoted instructions',
        findings: [], history: [], actions: ['approve_security', 'reject'],
        obsidian_url: `obsidian://open?vault=Research&file=${id}.pdf`, ...overrides};
}
const ok = body => ({ok: true, json: async () => body});
const accepted = body => ok({item: {artifact_id: body.artifact_id, status: body.action === 'reject' ? 'rejected' : 'pending'}});

async function panel(initialItems, post = accepted) {
    const nodes = new Map([['review', new Element('section', 'review')]]);
    for (const [, tag, id] of template.matchAll(/<(\w+)[^>]*\bid="([^"]+)"/g)) nodes.set(id, new Element(tag, id));
    nodes.get('review-status').value = 'active';
    const state = {items: structuredClone(initialItems), posts: [], gets: [], failRefresh: false};
    const context = vm.createContext({document: {
        getElementById: id => { assert.ok(nodes.has(id), `Template is missing ${id}`); return nodes.get(id); },
        createElement: tag => new Element(tag), addEventListener() {},
    }, fetch: async (url, options = {}) => {
        if (url.startsWith('/static/')) return {ok: true, text: async () => template};
        if (options.method === 'POST') {
            assert.equal(url, '/api/review/decision');
            assert.equal(options.headers['X-Thoth-Review'], '1');
            assert.equal(options.headers['Content-Type'], 'application/json');
            const body = JSON.parse(options.body);
            state.posts.push(body);
            return post(body, state.posts.length, state);
        }
        state.gets.push(url);
        if (state.failRefresh) throw new Error('Inbox offline');
        return ok({items: structuredClone(state.items), has_more: true});
    }});
    vm.runInContext(script, context, {filename: 'review.js'});
    await context.activate();
    return {context, state, get: id => nodes.get(`review-${id}`)};
}

function rowAction(ui, index, label) {
    return ui.get('items').children[index].children.at(-1).children.find(button => button.textContent === label);
}
async function selectAll(ui) {
    ui.get('select-all').checked = true;
    await ui.get('select-all').fire('change');
}
async function openBulk(ui) {
    await selectAll(ui);
    await ui.get('approve-selected').fire('click');
}

test('bulk selection only approves eligible items, with one confirmation and exact revisions', async () => {
    const sources = [item('a'), item('b'), item('c', {actions: ['retry', 'reject'], ocr_required: true}), item('d', {actions: []})];
    const ui = await panel(sources);
    assert.equal(ui.get('approve-selected').disabled, true);
    assert.equal(ui.get('items').children[2].children[0].disabled, true);
    assert.equal(ui.get('items').children[3].children[0].disabled, true);
    await openBulk(ui);
    assert.equal(ui.get('approve-selected').textContent, 'Approve selected (2)');
    assert.equal(ui.get('decision-dialog').modalCount, 1);
    assert.match(ui.get('decision-explanation').textContent, /one at a time.*Earlier approvals remain recorded/);
    assert.equal(ui.state.posts.length, 0);
    assert.doesNotMatch(template, /review-(actor|reason|ack)\b|I inspected/);
    await ui.get('decision-form').fire('submit');
    assert.deepEqual(ui.state.posts, sources.slice(0, 2).map(source => ({
        artifact_id: source.artifact_id, revision: source.revision, action: 'approve_security', security_acknowledged: true,
    })));
    assert.equal(ui.get('decision-dialog').open, false);
    assert.equal(ui.get('decision-dialog').modalCount, 1);
    assert.match(ui.get('decision-progress').textContent, /^2 of 2 decisions recorded/);
    assert.ok(ui.get('decision-results').children.every(result => result.textContent.endsWith('Approval recorded')));
    assert.equal(ui.get('confirm').hidden, true);
    assert.equal(ui.get('approve-selected').disabled, true);
});

for (const failure of ['stale revision', 'lost response', 'bad JSON', 'wrong item', 'wrong status', 'server error']) {
    test(`bulk stops on ${failure}, preserves partial results, and never resubmits`, async () => {
        const ui = await panel([item('a'), item('b'), item('c')], (body, count, state) => {
            if (count === 1) return accepted(body);
            // A refreshed revision is available, but it must not be silently approved.
            state.items[1].revision = 'f'.repeat(64);
            if (failure === 'stale revision') return {ok: false, status: 409, json: async () => ({detail: 'Review item changed'})};
            if (failure === 'lost response') throw new Error('Connection lost after sending');
            if (failure === 'bad JSON') return {ok: true, json: async () => { throw new Error('Invalid JSON'); }};
            if (failure === 'wrong item') return ok({item: {artifact_id: 'other', status: 'pending'}});
            if (failure === 'wrong status') return ok({item: {artifact_id: body.artifact_id, status: 'needs_review'}});
            return {ok: false, status: 500, json: async () => { throw new Error('HTML proxy response'); }};
        });
        await openBulk(ui);
        await ui.get('decision-form').fire('submit');
        assert.deepEqual(ui.state.posts.map(body => body.artifact_id), ['a', 'b']);
        assert.equal(ui.state.posts[1].revision, 'b'.repeat(64));
        assert.match(ui.get('decision-results').children[0].textContent, /Approval recorded$/);
        assert.match(ui.get('decision-results').children[1].textContent, /Not confirmed:/);
        assert.match(ui.get('decision-results').children[2].textContent, /Not attempted$/);
        assert.match(ui.get('decision-progress').textContent, /^Stopped: 1 of 3 decisions confirmed/);
        assert.match(ui.get('message').textContent, /^Stopped:/);
        assert.equal(ui.get('confirm').hidden, true);
        await ui.get('decision-form').fire('submit');
        assert.equal(ui.state.posts.length, 2);
    });
}

test('in-flight decisions report progress, block duplicate submits and cannot be dismissed', async () => {
    let finishFirst;
    const ui = await panel([item('a'), item('b')], (body, count) => count === 1
        ? new Promise(resolve => { finishFirst = () => resolve(accepted(body)); }) : accepted(body));
    await openBulk(ui);
    const submission = ui.get('decision-form').fire('submit');
    assert.match(ui.get('decision-results').children[0].textContent, /Sending/);
    assert.match(ui.get('decision-results').children[1].textContent, /Not attempted/);
    assert.equal(ui.get('confirm').disabled, true);
    assert.equal(ui.get('cancel').disabled, true);
    assert.equal((await ui.get('decision-dialog').fire('cancel')).defaultPrevented, true);
    await ui.get('decision-form').fire('submit');
    await ui.context.load();
    assert.equal(ui.state.posts.length, 1);
    assert.equal(ui.state.gets.length, 1);
    finishFirst();
    await submission;
    assert.equal(ui.state.posts.length, 2);
    assert.equal(ui.get('cancel').disabled, false);
});

test('a first failure leaves every subsequent item unattempted even when refresh also fails', async () => {
    const ui = await panel([item('a'), item('b')], (body, count, state) => {
        state.failRefresh = true;
        throw new Error('Connection lost');
    });
    await openBulk(ui);
    await ui.get('decision-form').fire('submit');
    assert.equal(ui.state.posts.length, 1);
    assert.match(ui.get('decision-results').children[1].textContent, /Not attempted$/);
    assert.match(ui.get('decision-progress').textContent, /^Stopped: 0 of 2/);
    assert.match(ui.get('message').textContent, /Stopped: 0 of 2.*\nCould not load the inbox/);
    assert.equal(ui.get('approve-selected').disabled, true);
});

test('an incomplete inbox render cannot leave invisible approvals selected', async () => {
    const ui = await panel([item('a'), item('b', {actions: null})]);
    assert.match(ui.get('message').textContent, /Could not load the inbox/);
    assert.equal(ui.get('items').children.length, 0);
    assert.equal(ui.get('select-all').disabled, true);
    assert.equal(ui.get('approve-selected').disabled, true);
    assert.equal(ui.state.posts.length, 0);
});

for (const [action, label] of [['approve_security', 'Approve'], ['retry', 'Retry extraction'], ['reject', 'Reject']]) {
    test(`single ${action} needs one confirmation and cancel sends nothing`, async () => {
        const source = item('a', {actions: [action], ocr_required: action === 'retry',
            action_note: 'Retry repeats text extraction; it does not run OCR. Rescan changed PDFs.'});
        const ui = await panel([source]);
        await rowAction(ui, 0, label).fire('click');
        assert.equal(ui.state.posts.length, 0);
        if (action === 'retry') assert.match(ui.get('decision-explanation').textContent, /does not run OCR/);
        if (action === 'approve_security') assert.match(ui.get('decision-explanation').textContent, /may send permitted content to model providers/);
        await ui.get('cancel').fire('click');
        await ui.get('decision-form').fire('submit');
        assert.equal(ui.state.posts.length, 0);
        await rowAction(ui, 0, label).fire('click');
        await ui.get('decision-form').fire('submit');
        assert.deepEqual(ui.state.posts, [{artifact_id: source.artifact_id, revision: source.revision,
            action, security_acknowledged: action === 'approve_security'}]);
        assert.equal(ui.get('decision-dialog').open, false);
        assert.match(ui.get('decision-progress').textContent, /^1 of 1 decisions recorded/);
    });
}

test('selection supports partial selection, deselect-all, and clearing across pages and refreshes', async () => {
    const ui = await panel([item('a'), item('b')]);
    const checkbox = ui.get('items').children[0].children[0];
    checkbox.checked = true;
    await checkbox.fire('change');
    assert.equal(ui.get('select-all').indeterminate, true);
    assert.equal(ui.get('approve-selected').textContent, 'Approve selected (1)');
    await selectAll(ui);
    ui.get('select-all').checked = false;
    await ui.get('select-all').fire('change');
    assert.equal(ui.get('approve-selected').disabled, true);
    await selectAll(ui);
    await ui.get('refresh').fire('click');
    assert.equal(ui.get('approve-selected').disabled, true);
    await selectAll(ui);
    await ui.get('next').fire('click');
    // The event callback schedules load; finish the pending response microtasks.
    await new Promise(resolve => setImmediate(resolve));
    assert.match(ui.state.gets.at(-1), /offset=100$/);
    assert.equal(ui.get('approve-selected').disabled, true);
});

test('untrusted titles, reasons and history stay text, and unsafe links stay inert', async () => {
    const hostile = '<img src=x onerror=alert(1)>';
    const ui = await panel([item('a', {title: hostile, reason: hostile, reason_summary: hostile,
        obsidian_url: 'javascript:alert(1)', source_url: 'data:text/html,bad',
        history: [{at: 'today', actor: hostile, action: 'reject', reason: hostile}]})]);
    const row = ui.get('items').children[0];
    assert.ok(row.textContent.includes(hostile));
    assert.equal(row.children[3].textContent, 'No link');
    await rowAction(ui, 0, 'Details').fire('click');
    assert.equal(ui.get('details-dialog').open, true);
    assert.ok(ui.get('details-content').textContent.includes(hostile));
    assert.equal(ui.state.posts.length, 0);
    const web = await panel([item('a', {source_url: 'https://example.org/paper'})]);
    const link = web.get('items').children[0].children[3].children[1];
    assert.equal(link.href, 'https://example.org/paper');
    assert.equal(link.rel, 'noopener noreferrer');
});
