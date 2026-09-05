'use strict';
// ES module: review state must not collide with the existing settings script.
const root = document.getElementById('review');
const $ = id => document.getElementById(`review-${id}`);
const labels = {approve_security: 'Approve flagged content…', retry: 'Retry processing…', reject: 'Reject processing…'};
let offset = 0;
let selected = null;
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

function renderItem(item) {
    const card = element('article', undefined, 'card review-item');
    card.append(element('h3', item.title, 'card-title'));
    card.append(element('p', `${item.status.replaceAll('_', ' ')} · ${item.source} · ${item.attempts} processing attempt(s)`, 'review-meta'));
    card.append(element('code', item.source_relative_path || item.source_path || item.artifact_id, 'review-path'));
    if (item.obsidian_url && item.obsidian_url.startsWith('obsidian://open?')) {
        const link = element('a', 'Open original in Obsidian');
        link.href = item.obsidian_url;
        card.append(link);
    }
    card.append(element('p', item.reason || 'No reason was recorded.', 'review-reason'));
    for (const finding of item.findings) card.append(element('span', `${finding.pattern_id} · ${finding.severity}`, 'review-finding'));
    if (item.action_note) card.append(element('p', item.action_note));
    const actions = element('div', undefined, 'btn-group');
    for (const action of item.actions) {
        const button = element('button', labels[action], action === 'reject' ? 'btn btn-danger' : 'btn btn-primary');
        button.type = 'button';
        button.addEventListener('click', () => openDecision(item, action));
        actions.append(button);
    }
    card.append(actions);
    const details = element('details');
    details.append(element('summary', 'Processing details & decision history'));
    details.append(element('p', `Source checksum: ${item.source_checksum || 'not recorded'}`, 'review-path'));
    if (item.last_error) details.append(element('p', item.last_error, 'review-history'));
    details.append(element('div', item.history.map(event => `${event.at} · ${event.actor || 'system'} · ${event.action}\n${event.reason}`).join('\n\n') || 'No decision history recorded.', 'review-history'));
    card.append(details);
    return card;
}

async function load() {
    if (loading) return;
    loading = true;
    $('refresh').disabled = $('status').disabled = $('previous').disabled = $('next').disabled = true;
    $('message').textContent = 'Loading review items…';
    $('items').replaceChildren();
    $('count').textContent = '';
    try {
        const data = await request(`/api/review?status=${encodeURIComponent($('status').value)}&limit=100&offset=${offset}`);
        $('items').replaceChildren(...data.items.map(renderItem));
        $('count').textContent = data.items.length ? `Showing ${offset + 1}–${offset + data.items.length}${data.has_more ? ' · more available' : ''}` : '0 items';
        $('message').textContent = data.items.length ? '' : 'No items in this view. Nothing has been approved or deleted.';
        $('previous').disabled = offset === 0;
        $('next').disabled = !data.has_more;
    } catch (error) { $('message').textContent = `Could not load the inbox: ${error.message}`; }
    finally { loading = false; $('refresh').disabled = $('status').disabled = false; }
}

function openDecision(item, action) {
    selected = {item, action};
    $('decision-title').textContent = labels[action].replace('…', '');
    $('decision-source').textContent = item.title;
    $('decision-explanation').textContent = action === 'reject'
        ? 'Keep the original and its audit trail, but stop THOTH from processing this item. This does not delete the file.'
        : 'Return this revision to the processing queue. This is not a guarantee that processing will succeed; other checks still apply.';
    $('reason').value = '';
    $('ack').checked = false;
    $('ack').required = action === 'approve_security';
    $('ack-label').hidden = action !== 'approve_security';
    $('decision-error').textContent = '';
    $('decision-dialog').showModal();
}

function bindControls() {
$('decision-form').addEventListener('submit', async event => {
    event.preventDefault();
    if (sending || !selected) return;
    sending = true;
    $('confirm').disabled = $('cancel').disabled = true;
    try {
        await request('/api/review/decision', {method: 'POST', headers: {'Content-Type': 'application/json', 'X-Thoth-Review': '1'},
            body: JSON.stringify({artifact_id: selected.item.artifact_id, revision: selected.item.revision,
                action: selected.action, actor: $('actor').value, reason: $('reason').value,
                security_acknowledged: $('ack').checked})});
        $('decision-dialog').close();
        selected = null;
        await load();
        $('message').textContent = 'Decision recorded. The original file is unchanged.';
    } catch (error) { $('decision-error').textContent = `${error.message}. Close this dialog and refresh if the item changed.`; }
    finally { sending = false; $('confirm').disabled = $('cancel').disabled = false; }
});
$('cancel').addEventListener('click', () => $('decision-dialog').close());
$('decision-dialog').addEventListener('cancel', event => { if (sending) event.preventDefault(); });
$('refresh').addEventListener('click', load);
$('status').addEventListener('change', () => { offset = 0; load(); });
$('previous').addEventListener('click', () => { offset = Math.max(0, offset - 100); load(); });
$('next').addEventListener('click', () => { offset += 100; load(); });
}

async function initialize() {
    const response = await fetch('/static/review-panel.html?v=2', {cache: 'no-cache'});
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
