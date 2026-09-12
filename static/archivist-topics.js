function archivistEscapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function archivistFormatOptionalDate(value) {
    if (!value) return 'Never';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function renderArchivistPills(values) {
    if (!Array.isArray(values) || values.length === 0) {
        return '<span class="archivist-pill">None</span>';
    }
    return values
        .map(value => `<span class="archivist-pill">${archivistEscapeHtml(value)}</span>`)
        .join('');
}

function renderArchivistTopics(topics) {
    const container = document.getElementById('archivist-topics-list');
    if (!container) return;

    if (!Array.isArray(topics) || topics.length === 0) {
        container.innerHTML = '<div class="archivist-empty">No archivist topics are defined yet.</div>';
        return;
    }

    container.innerHTML = topics.map(topic => {
        const state = topic.state || {};
        const stateError = topic.state_error
            ? `<div class="archivist-state-box error-state">${archivistEscapeHtml(topic.state_error)}</div>`
            : `
                <div class="archivist-state-box">
                    <div><strong>Last Success</strong> ${archivistEscapeHtml(archivistFormatOptionalDate(state.last_success_at))}</div>
                    <div><strong>Last Run</strong> ${archivistEscapeHtml(archivistFormatOptionalDate(state.last_run_at))}</div>
                    <div><strong>Next Due</strong> ${archivistEscapeHtml(archivistFormatOptionalDate(state.next_due_at))}</div>
                    <div><strong>Last Route</strong> ${archivistEscapeHtml(
                        state.last_model_provider
                            ? `${state.last_model_provider} / ${state.last_model || 'default'}`
                            : 'Never run'
                    )}</div>
                    <div><strong>Last Candidate Count</strong> ${archivistEscapeHtml(String(state.last_candidate_count || 0))}</div>
                    <div><strong>Force Queued</strong> ${archivistEscapeHtml(archivistFormatOptionalDate(state.force_requested_at))}</div>
                </div>
            `;

        const forceButtons = topic.allow_manual_force
            ? `
                <div class="btn-group">
                    <button class="btn btn-secondary btn-small" onclick="runArchivistTopic('${archivistEscapeHtml(topic.id)}')">Run Now</button>
                    <button class="btn btn-secondary btn-small" onclick="queueArchivistForce('${archivistEscapeHtml(topic.id)}')">Force Run</button>
                    <button class="btn btn-secondary btn-small" onclick="clearArchivistForce('${archivistEscapeHtml(topic.id)}')">Clear Force</button>
                </div>
            `
            : `
                <div class="btn-group">
                    <button class="btn btn-secondary btn-small" onclick="runArchivistTopic('${archivistEscapeHtml(topic.id)}')">Run Now</button>
                </div>
            `;

        return `
            <article class="archivist-topic-card">
                <div class="archivist-topic-main">
                    <h3>${archivistEscapeHtml(topic.title)}</h3>
                    <div class="topic-id">${archivistEscapeHtml(topic.id)}</div>
                    ${topic.description ? `<p>${archivistEscapeHtml(topic.description)}</p>` : ''}
                    <div class="archivist-topic-meta">
                        <div><strong>Output</strong>${archivistEscapeHtml(topic.output_path)}</div>
                        <div><strong>Cadence</strong>${archivistEscapeHtml(String(topic.cadence_hours))}h</div>
                        <div><strong>Max Sources</strong>${archivistEscapeHtml(topic.max_sources == null ? 'Unlimited' : String(topic.max_sources))}</div>
                        <div><strong>Manual Force</strong>${topic.allow_manual_force ? 'Allowed' : 'Disabled'}</div>
                        <div><strong>Retrieval</strong>${archivistEscapeHtml(topic.retrieval?.mode || 'literal')}</div>
                    </div>
                    ${stateError}
                    <details class="archivist-topic-details">
                        <summary>Filters &amp; weights</summary>
                        <div class="archivist-topic-detail-panel" tabindex="0" aria-label="Filters and weights for ${archivistEscapeHtml(topic.title)}">
                            <div class="archivist-section-label">Include Roots</div>
                            <div class="archivist-pill-list">${renderArchivistPills(topic.include_roots)}</div>
                            <div class="archivist-section-label">Exclude Roots</div>
                            <div class="archivist-pill-list">${renderArchivistPills(topic.exclude_roots)}</div>
                            <div class="archivist-section-label">Source Types</div>
                            <div class="archivist-pill-list">${renderArchivistPills(topic.source_types)}</div>
                            <div class="archivist-section-label">Include Tags</div>
                            <div class="archivist-pill-list">${renderArchivistPills(topic.include_tags)}</div>
                            <div class="archivist-section-label">Exclude Tags</div>
                            <div class="archivist-pill-list">${renderArchivistPills(topic.exclude_tags)}</div>
                            <div class="archivist-section-label">Include Terms</div>
                            <div class="archivist-pill-list">${renderArchivistPills(topic.include_terms)}</div>
                            <div class="archivist-section-label">Exclude Terms</div>
                            <div class="archivist-pill-list">${renderArchivistPills(topic.exclude_terms)}</div>
                            <div class="archivist-section-label">Retrieval Policy</div>
                            <div class="archivist-pill-list">${renderArchivistPills([
                                `mode:${topic.retrieval?.mode || 'literal'}`,
                                `tags:${topic.retrieval?.tag_mode || 'required'}`,
                                `terms:${topic.retrieval?.term_mode || 'required'}`,
                                `fts:${topic.retrieval?.full_text_limit ?? 'n/a'}`,
                                `semantic:${topic.retrieval?.semantic_limit ?? 'n/a'}`,
                                `rerank:${topic.retrieval?.rerank_limit ?? 'n/a'}`,
                                `embed_budget:${topic.retrieval?.max_new_embeddings_per_run ?? 'n/a'}`
                            ])}</div>
                            <div class="archivist-section-label">Source Type Weights</div>
                            <div class="archivist-pill-list">${renderArchivistPills(
                                Object.entries(topic.retrieval?.source_type_weights || {}).map(
                                    ([key, value]) => `${key}:${value}`
                                )
                            )}</div>
                        </div>
                    </details>
                </div>
                ${forceButtons}
            </article>
        `;
    }).join('');
}
