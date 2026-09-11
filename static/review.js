'use strict';
// ES module: review state must not collide with the existing settings script.
const root = document.getElementById('review');
const $ = id => document.getElementById(`review-${id}`);
const labels = {approve_security: 'Approve', retry: 'Retry', reject: 'Reject'};
const selection = new Map();
const rowChecks = new Map();
let items = [];
let offset = 0;
let pendingDecision = null;
let loading = false;
let sending = false;
let initialization = null;

function element(tag, text, className) {
    const el = document.createElement(tag);
    if (text !== undefined) el.textContent = text;
    if (className) el.className = className;
    return el;
}

async function request(path, options = {}) {
    const response = await fetch(path, {cache: 'no-store', ...options});
    if (!response.ok) {
        let detail = `Request failed (${response.status})`;
        try { const body = await response.json(); if (typeof body.detail === 'string') detail = body.detail; }
        catch { /* Retain the explicit HTTP failure when a proxy returns HTML. */ }
        throw new Error(detail);
    }
    return response.json();
}

function sourceLinks(item) {
    const links = element('span', undefined, 'review-links');
    if (typeof item.obsidian_url === 'string' && item.obsidian_url.startsWith('obsidian://open?')) {
        const link = element('a', 'Open');
        link.href = item.obsidian_url;
        link.title = 'Open original in Obsidian';
        links.append(link);
    }
    if (typeof item.source_url === 'string' && /^https?:\/\//i.test(item.source_url)) {
        const link = element('a', 'Source');
        link.href = item.source_url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        links.append(link);
    }
    if (!links.children.length) links.textContent = 'No link';
    return links;
}

function openDetails(item) {
    $('details-title').textContent = item.title;
    const content = $('details-content');
    content.replaceChildren(
        element('p', `${item.status.replaceAll('_', ' ')} · ${item.source} · ${item.attempts} processing attempt(s)`, 'review-meta'),
        element('p', item.source_relative_path || item.source_path || item.artifact_id, 'review-path'),
        sourceLinks(item),
        element('h3', 'Why it paused'),
        element('p', item.reason || 'No reason was recorded.', 'review-history'),
    );
    for (const finding of item.findings) content.append(element('span', `${finding.pattern_id} · ${finding.severity}`, 'review-finding'));
    if (item.action_note) content.append(element('p', item.action_note, 'review-history'));
    content.append(element('p', `Source checksum: ${item.source_checksum || 'not recorded'}`, 'review-path'));
    if (item.last_error && item.last_error !== item.reason) content.append(element('p', item.last_error, 'review-history'));
    content.append(element('h3', 'Decision history'));
    content.append(element('p', item.history.map(event => `${event.at} · ${event.actor || 'system'} · ${event.action}\n${event.reason}`).join('\n\n') || 'No decision history recorded.', 'review-history'));
    $('details-dialog').showModal();
}

function updateSelection() {
    const available = items.filter(item => item.actions.includes('approve_security'));
    $('select-all').disabled = loading || sending || !available.length;
    $('select-all').checked = available.length > 0 && selection.size === available.length;
    $('select-all').indeterminate = selection.size > 0 && selection.size < available.length;
    $('approve-selected').disabled = loading || sending || !selection.size;
    $('approve-selected').textContent = `Approve selected (${selection.size})`;
    for (const [id, checkbox] of rowChecks) checkbox.checked = selection.has(id);
}

function actionLabel(item, action) {
    return action === 'retry' && item.ocr_required ? 'Retry extraction' : labels[action];
}

function renderItem(item) {
    const row = element('article', undefined, 'review-item');
    const checkbox = element('input');
    checkbox.type = 'checkbox';
    checkbox.disabled = !item.actions.includes('approve_security');
    checkbox.setAttribute('aria-label', `Select ${item.title} for approval`);
    if (checkbox.disabled) checkbox.title = 'No security approval is available for this item.';
    checkbox.addEventListener('change', () => {
        if (checkbox.checked) selection.set(item.artifact_id, item);
        else selection.delete(item.artifact_id);
        updateSelection();
    });
    rowChecks.set(item.artifact_id, checkbox);
    const path = item.source_relative_path || item.source_path || '';
    const filename = path.split('/').pop();
    const title = element('span', filename && filename !== item.title ? `${filename} · ${item.title}` : item.title, 'review-name');
    title.title = `${item.title}\n${path || item.artifact_id}`;
    const reason = element('span', item.reason_summary || item.reason || 'No reason recorded.', 'review-reason');
    reason.title = item.action_note || item.reason;
    const actions = element('div', undefined, 'review-actions');
    for (const action of item.actions) {
        const button = element('button', actionLabel(item, action), `btn btn-small ${action === 'reject' ? 'btn-danger' : 'btn-primary'}`);
        button.type = 'button';
        button.addEventListener('click', () => openDecision([{item, action}]));
        actions.append(button);
    }
    const details = element('button', 'Details', 'btn btn-secondary btn-small');
    details.type = 'button';
    details.setAttribute('aria-label', `Details for ${item.title}`);
    details.addEventListener('click', () => openDetails(item));
    actions.append(details);
    row.append(checkbox, title, reason, sourceLinks(item), actions);
    return row;
}

async function load(announcement = '') {
    if (loading || sending || pendingDecision) return;
    loading = true;
    selection.clear();
    rowChecks.clear();
    items = [];
    updateSelection();
    $('refresh').disabled = $('status').disabled = $('previous').disabled = $('next').disabled = true;
    $('message').textContent = announcement || 'Loading review items…';
    $('items').replaceChildren();
    $('count').textContent = '';
    try {
        const data = await request(`/api/review?status=${encodeURIComponent($('status').value)}&limit=100&offset=${offset}`);
        items = data.items;
        $('items').replaceChildren(...items.map(renderItem));
        $('items').scrollTop = 0;
        $('count').textContent = items.length ? `Showing ${offset + 1}–${offset + items.length}${data.has_more ? ' · more available' : ''}` : '0 items';
        $('message').textContent = announcement || (items.length ? '' : 'No items in this view.');
        $('previous').disabled = offset === 0;
        $('next').disabled = !data.has_more;
    } catch (error) {
        // An incomplete render must not leave invisible items available for bulk approval.
        items = [];
        rowChecks.clear();
        $('items').replaceChildren();
        $('message').textContent = `${announcement ? announcement + '\n' : ''}Could not load the inbox: ${error.message}`;
    }
    finally { loading = false; $('refresh').disabled = $('status').disabled = false; updateSelection(); }
}

function openDecision(decisions) {
    if (loading || sending || pendingDecision || !decisions.length) return;
    // Freeze precisely the revisions shown; never fetch a new revision to retry a decision.
    pendingDecision = decisions.map(({item, action}) => ({artifact_id: item.artifact_id, revision: item.revision,
        title: item.title, source: item.source_relative_path || item.source_path || item.artifact_id, action}));
    const {item, action} = decisions[0];
    const bulk = decisions.length > 1;
    $('decision-title').textContent = bulk ? `Approve ${decisions.length} items?` : `${actionLabel(item, action)}?`;
    $('decision-source').textContent = bulk ? `${decisions.length} selected sources on this page` : `${item.title} · ${pendingDecision[0].source}`;
    $('decision-explanation').textContent = action === 'reject'
        ? 'Stop processing this item and keep its original file and decision history.'
        : action === 'approve_security'
            ? 'Approve processing of this flagged content. Source text remains untrusted. The configured pipeline may send permitted content to model providers. Other processing checks still apply.'
            : `Queue this captured version for another processing attempt. ${item.action_note || 'Other processing checks still apply; success is not guaranteed.'}`;
    if (bulk) $('decision-explanation').textContent += ' Approvals run one at a time and stop at the first problem. Earlier approvals remain recorded; remaining items are not attempted.';
    $('decision-progress').textContent = '';
    $('decision-results').replaceChildren();
    $('decision-results').hidden = true;
    $('confirm').textContent = bulk ? `Approve ${decisions.length} items` : actionLabel(item, action);
    $('confirm').hidden = false;
    $('confirm').disabled = false;
    $('cancel').textContent = 'Cancel';
    $('decision-dialog').showModal();
}

async function submitDecision(event) {
    event.preventDefault();
    if (sending || !pendingDecision) return;
    const decisions = pendingDecision;
    pendingDecision = null;
    sending = true;
    $('confirm').disabled = $('cancel').disabled = true;
    const resultLabel = decision => `${decision.title} (${decision.source})`;
    const results = decisions.map(decision => element('li', `${resultLabel(decision)} — Not attempted`));
    $('decision-results').replaceChildren(...results);
    $('decision-results').hidden = false;
    let confirmed = 0;
    let stopped = false;
    for (const [index, decision] of decisions.entries()) {
        results[index].textContent = `${resultLabel(decision)} — Sending…`;
        $('decision-progress').textContent = `Recording decision ${index + 1} of ${decisions.length}…`;
        try {
            const data = await request('/api/review/decision', {
                method: 'POST', headers: {'Content-Type': 'application/json', 'X-Thoth-Review': '1'},
                body: JSON.stringify({artifact_id: decision.artifact_id, revision: decision.revision,
                    action: decision.action, security_acknowledged: decision.action === 'approve_security'}),
            });
            // A lost/malformed response may follow a committed write. Do not call it success or retry it.
            if (data?.item?.artifact_id !== decision.artifact_id || data.item.status !== (decision.action === 'reject' ? 'rejected' : 'pending')) {
                throw new Error('The server did not confirm the expected decision');
            }
            confirmed += 1;
            results[index].textContent = `${resultLabel(decision)} — ${decision.action === 'approve_security' ? 'Approval' : 'Decision'} recorded`;
        } catch (error) {
            results[index].textContent = `${resultLabel(decision)} — Not confirmed: ${error.message}`;
            stopped = true;
            break;
        }
    }
    const summary = stopped
        ? `Stopped: ${confirmed} of ${decisions.length} decisions confirmed. Check the results and refresh the inbox before deciding again; an unconfirmed decision may have been recorded. Earlier decisions remain recorded.`
        : `${confirmed} of ${decisions.length} decisions recorded. Original files are unchanged.`;
    $('decision-progress').textContent = summary;
    $('confirm').hidden = true;
    $('cancel').disabled = false;
    $('cancel').textContent = 'Close';
    $('cancel').focus();
    sending = false;
    await load(summary);
}

function bindControls() {
    $('decision-form').addEventListener('submit', submitDecision);
    $('cancel').addEventListener('click', () => { pendingDecision = null; $('decision-dialog').close(); });
    $('decision-dialog').addEventListener('cancel', event => {
        if (sending) event.preventDefault();
        else pendingDecision = null;
    });
    $('select-all').addEventListener('change', () => {
        selection.clear();
        if ($('select-all').checked) {
            for (const item of items) if (item.actions.includes('approve_security')) selection.set(item.artifact_id, item);
        }
        updateSelection();
    });
    $('approve-selected').addEventListener('click', () => openDecision(items.filter(item => selection.has(item.artifact_id)).map(item => ({item, action: 'approve_security'}))));
    $('refresh').addEventListener('click', () => load());
    $('status').addEventListener('change', () => { offset = 0; load(); });
    $('previous').addEventListener('click', () => { offset = Math.max(0, offset - 100); load(); });
    $('next').addEventListener('click', () => { offset += 100; load(); });
}

async function initialize() {
    const response = await fetch('/static/review-panel.html?v=3', {cache: 'no-cache'});
    if (!response.ok) throw new Error(`Review panel unavailable (${response.status})`);
    // This is our fixed, same-origin UI template, never an ingestion payload.
    root.innerHTML = await response.text();
    bindControls();
}

async function activate() {
    try {
        if (!initialization) {
            initialization = initialize().catch(error => { initialization = null; throw error; });
        }
        await initialization;
        await load();
    } catch (error) {
        const message = element('p', `Could not open Review: ${error.message}`, 'card-description');
        const retry = element('button', 'Retry loading Review', 'btn btn-secondary');
        retry.addEventListener('click', activate);
        root.replaceChildren(message, retry);
    }
}

document.addEventListener('thoth:tab-changed', event => { if (event.detail === 'review') activate(); });
if (root.classList.contains('active')) activate();
