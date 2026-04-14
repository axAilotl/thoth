// ==UserScript==
// @name         Thoth Live Capture
// @namespace    http://tampermonkey.net/
// @version      0.2.0
// @description  Capture Twitter/X bookmark GraphQL mutations and send to local Thoth API
// @author       you
// @match        https://twitter.com/*
// @match        https://x.com/*
// @grant        GM_xmlhttpRequest
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_notification
// @grant        unsafeWindow
// @connect      localhost
// @connect      127.0.0.1
// @run-at       document-start
// ==/UserScript==

(function () {
    'use strict';

    const THOTH_API_URL = 'http://127.0.0.1:8090';
    const STORAGE_KEYS = {
        QUEUE: 'thoth_queue',
        LAST_SENT: 'thoth_last_sent',
        PROCESSED_BASE: 'thoth_processed_base',
        PROCESSED_FULL: 'thoth_processed_full'
    };

    // Cache for recent GraphQL responses
    const graphqlCache = new Map(); // tweet_id -> {response, timestamp}
    const CACHE_EXPIRY_MS = 5 * 60 * 1000; // 5 minutes
    const MAX_CACHE_SIZE = 5; // Keep only last 5 responses

    const pendingGraphTweets = new Set();
    const knownBookmarks = new Set(); // Track tweets we know are bookmarked

    const log = (...args) => console.log('[Thoth]', ...args);

    function nowIso() {
        return new Date().toISOString();
    }

    function loadQueue() {
        try {
            return JSON.parse(GM_getValue(STORAGE_KEYS.QUEUE, '[]'));
        } catch (_) { return []; }
    }

    function saveQueue(q) {
        GM_setValue(STORAGE_KEYS.QUEUE, JSON.stringify(q.slice(-1000)));
    }

    function getProcessedBaseSet() {
        try { return new Set(JSON.parse(GM_getValue(STORAGE_KEYS.PROCESSED_BASE, '[]'))); } catch (_) { return new Set(); }
    }
    function getProcessedFullSet() {
        try { return new Set(JSON.parse(GM_getValue(STORAGE_KEYS.PROCESSED_FULL, '[]'))); } catch (_) { return new Set(); }
    }
    function saveProcessedBaseSet(set) { GM_setValue(STORAGE_KEYS.PROCESSED_BASE, JSON.stringify(Array.from(set).slice(-5000))); }
    function saveProcessedFullSet(set) { GM_setValue(STORAGE_KEYS.PROCESSED_FULL, JSON.stringify(Array.from(set).slice(-5000))); }

    async function apiHealth() {
        return new Promise((resolve) => {
            GM_xmlhttpRequest({
                method: 'GET',
                url: THOTH_API_URL + '/health',
                onload: (res) => resolve(res.status === 200),
                onerror: () => resolve(false),
                timeout: 2000,
                ontimeout: () => resolve(false)
            });
        });
    }

    function enqueue(payload) {
        const q = loadQueue();
        q.push(payload);
        saveQueue(q);
        GM_notification({ title: 'Thoth', text: 'Queued bookmark ' + payload.tweet_id, timeout: 1500 });
    }

    function postBookmark(payload) {
        return new Promise((resolve, reject) => {
            GM_xmlhttpRequest({
                method: 'POST',
                url: THOTH_API_URL + '/api/bookmark',
                headers: { 'Content-Type': 'application/json' },
                data: JSON.stringify(payload),
                onload: (res) => {
                    if (res.status >= 200 && res.status < 300) {
                        resolve(JSON.parse(res.responseText));
                    } else {
                        reject(new Error('HTTP ' + res.status));
                    }
                },
                onerror: () => reject(new Error('Network error'))
            });
        });
    }

    async function flushQueue() {
        const healthy = await apiHealth();
        if (!healthy) return;
        let q = loadQueue();
        if (!q.length) return;
        const next = q[0];
        try {
            await postBookmark(next);
            q.shift();
            saveQueue(q);
        } catch (_) {
            // keep in queue
        }
    }

    function extractTweetIdFromGraphQL(bodyObj) {
        try {
            // Twitter bookmark mutation typically includes variables.tweet_id or tweetId
            const v = bodyObj.variables || {};
            return String(v.tweet_id || v.tweetId || v.focalTweetId || v.statusId || '');
        } catch (_) {
            return '';
        }
    }

    function extractTweetIdFromUrl(url) {
        try {
            const u = new URL(url);
            const vars = u.searchParams.get('variables');
            if (vars) {
                const v = JSON.parse(vars);
                // TweetDetail uses focalTweetId; keep fallbacks for mutations/variants
                return String(v.tweet_id || v.tweetId || v.focalTweetId || v.statusId || '');
            }
        } catch (_) {}
        return '';
    }

    function extractTweetIdFromGraphQLResponse(respJson) {
        try {
            // Handle TweetDetail structure
            let instructions = respJson?.data?.threaded_conversation_with_injections_v2?.instructions;
            
            // Handle ModeratedTimeline structure
            if (!instructions) {
                instructions = respJson?.data?.tweet?.result?.timeline_response?.timeline?.instructions;
            }
            
            if (!instructions) return '';
            
            // Find TimelineAddEntries instruction
            const timelineAddEntries = instructions.find(i => i.type === 'TimelineAddEntries');
            const entries = timelineAddEntries?.entries || [];
            
            for (const entry of entries) {
                // Try entry ID pattern first
                const entryId = entry?.entryId || '';
                const m = /tweet-(\d{5,})/.exec(entryId);
                if (m) return m[1];

                const tryOne = (ic) => {
                    if (!ic) return '';
                    const result = ic?.tweet_results?.result
                                || ic?.tweet_results
                                || ic?.tweet
                                || ic?.result
                                || null;
                    const rest = result?.rest_id;
                    if (rest) return String(rest);
                    const legacy = result?.legacy?.id_str;
                    if (legacy) return String(legacy);
                    return '';
                };

                const idA = tryOne(entry?.content?.itemContent);
                if (idA) return idA;

                // Handle conversation threads
                const items = entry?.content?.items || [];
                for (const item of items) {
                    if (item.entryId && item.entryId.includes('-tweet-')) {
                        const idB = tryOne(item?.item?.itemContent);
                        if (idB) return idB;
                    }
                }
            }
        } catch (_) {}
        return '';
    }

    function findTweetResultInDetail(respJson, preferredId) {
        const instructions = respJson?.data?.threaded_conversation_with_injections_v2?.instructions || [];

        const tryOne = (itemContent) => {
            if (!itemContent) return null;
            const result = itemContent?.tweet_results?.result
                || itemContent?.tweet_results
                || itemContent?.tweet
                || itemContent?.result
                || null;
            if (!result) return null;
            const rest = result.rest_id || result.legacy?.id_str;
            if (!rest) return null;
            return { id: String(rest), result };
        };

        let fallback = null;

        for (const instruction of instructions) {
            const entries = instruction?.entries || [];
            for (const entry of entries) {
                const direct = tryOne(entry?.content?.itemContent);
                if (direct) {
                    if (!preferredId || direct.id === preferredId) return direct;
                    if (!fallback) fallback = direct;
                }

                const items = entry?.content?.items || [];
                for (const item of items) {
                    const nested = tryOne(item?.item?.itemContent);
                    if (nested) {
                        if (!preferredId || nested.id === preferredId) return nested;
                        if (!fallback) fallback = nested;
                    }
                }
            }
        }

        return fallback;
    }

    function isTweetBookmarked(respJson, tweetId) {
        // Check if the tweet in the GraphQL response is bookmarked
        const tweetInfo = findTweetResultInDetail(respJson, tweetId);
        if (!tweetInfo || !tweetInfo.result) return false;
        
        // Check the bookmarked field in the legacy object
        const isBookmarked = tweetInfo.result?.legacy?.bookmarked === true;
        if (isBookmarked) {
            log('🔖 Tweet is bookmarked:', tweetId);
        }
        return isBookmarked;
    }

    function cacheGraphQLResponse(tweetId, response) {
        if (!tweetId || !response) return;
        
        // If cache is full, remove oldest entry
        if (graphqlCache.size >= MAX_CACHE_SIZE && !graphqlCache.has(tweetId)) {
            // Find and remove the oldest entry
            let oldestId = null;
            let oldestTime = Date.now();
            for (const [id, data] of graphqlCache.entries()) {
                if (data.timestamp < oldestTime) {
                    oldestTime = data.timestamp;
                    oldestId = id;
                }
            }
            if (oldestId) {
                graphqlCache.delete(oldestId);
                log('🗑️ Evicted old cache entry:', oldestId);
            }
        }
        
        graphqlCache.set(tweetId, {
            response: response,
            timestamp: Date.now()
        });
        log('💾 Cached GraphQL response for', tweetId, '(cache size:', graphqlCache.size + ')');
        
        // Also try to extract and cache by all possible tweet IDs in the response
        // This ensures we can find it even if extraction methods differ
        try {
            const entries = response.data?.threaded_conversation_with_injections_v2?.instructions?.[0]?.entries || [];
            for (const entry of entries) {
                const entryId = entry?.entryId || '';
                const m = /tweet-(\d{5,})/.exec(entryId);
                if (m && m[1] !== tweetId) {
                    graphqlCache.set(m[1], {
                        response: response,
                        timestamp: Date.now()
                    });
                    log('💾 Also cached for tweet ID from entry:', m[1]);
                }
            }
        } catch (_) {}
        
        // Clean up expired entries
        const now = Date.now();
        for (const [id, data] of graphqlCache.entries()) {
            if (now - data.timestamp > CACHE_EXPIRY_MS) {
                graphqlCache.delete(id);
            }
        }
    }

    function getCachedGraphQLResponse(tweetId) {
        const cached = graphqlCache.get(tweetId);
        if (!cached) return null;
        
        const now = Date.now();
        if (now - cached.timestamp > CACHE_EXPIRY_MS) {
            graphqlCache.delete(tweetId);
            return null;
        }
        
        log('♻️ Using cached GraphQL response for', tweetId);
        return cached.response;
    }

    function isBookmarkMutation(url, bodyObj) {
        if (!/graphql/.test(url)) return false;
        // Check for CreateBookmark or DeleteBookmark mutations specifically
        return /\/CreateBookmark$/.test(url) || /\/DeleteBookmark$/.test(url);
    }

    function isTweetDetailQuery(url, bodyObj) {
        // Simple and effective pattern matching like twitter-exporter
        return /\/graphql\/.+\/TweetDetail/.test(url) || 
               /\/graphql\/.+\/ModeratedTimeline/.test(url);
    }

    function isBookmarksTimelineQuery(url) {
        return /\/graphql\/.+\/Bookmarks/.test(url);
    }

    function extractTweetDataFromResult(result) {
        if (!result || typeof result !== 'object') return { text: '', author: '' };
        const legacy = result.legacy || {};
        const coreUser = result.core?.user_results?.result?.legacy || {};
        const text = legacy.full_text || legacy.text || '';
        const author = coreUser.screen_name || legacy.screen_name || '';
        return { text, author };
    }

    function extractTweetsFromBookmarksResponse(respJson) {
        const tweets = [];
        const instructions = respJson?.data?.bookmark_timeline_v2?.timeline?.instructions || [];

        const handleResult = (result) => {
            if (!result) return;
            const id = result.rest_id || result.legacy?.id_str;
            if (!id) return;
            tweets.push({
                id: String(id),
                data: extractTweetDataFromResult(result)
            });
        };

        for (const instruction of instructions) {
            const entries = instruction?.entries || [];
            for (const entry of entries) {
                const content = entry?.content || {};
                const itemContent = content?.itemContent;
                if (itemContent?.tweet_results?.result) {
                    handleResult(itemContent.tweet_results.result);
                }
                const items = content?.items || [];
                for (const item of items) {
                    const ic = item?.item?.itemContent;
                    if (ic?.tweet_results?.result) {
                        handleResult(ic.tweet_results.result);
                    }
                }
            }
        }

        const seen = new Set();
        return tweets.filter(({ id }) => {
            if (seen.has(id)) return false;
            seen.add(id);
            return true;
        });
    }

    function fetchBookmarkStatuses(tweetIds) {
        return new Promise((resolve) => {
            if (!tweetIds.length) {
                resolve({});
                return;
            }
            GM_xmlhttpRequest({
                method: 'POST',
                url: THOTH_API_URL + '/api/bookmarks/status',
                headers: { 'Content-Type': 'application/json' },
                data: JSON.stringify({ tweet_ids: tweetIds }),
                onload: (res) => {
                    try {
                        if (res.status >= 200 && res.status < 300) {
                            const body = JSON.parse(res.responseText || '{}');
                            resolve(body.statuses || {});
                        } else {
                            resolve({});
                        }
                    } catch (_) { resolve({}); }
                },
                onerror: () => resolve({})
            });
        });
    }

    async function syncBookmarksFromTimeline(respJson) {
        try {
            const tweets = extractTweetsFromBookmarksResponse(respJson);
            if (!tweets.length) return;

        const statuses = await fetchBookmarkStatuses(tweets.map(t => t.id));
        for (const tweet of tweets) {
            const info = statuses[tweet.id] || {};
            const status = info.status || 'missing';
            const processedGraph = !!info.processed_with_graphql;

        if (status === 'missing' || status === 'failed') {
            log('📥 Bookmark timeline pending:', tweet.id, 'status:', status, 'processedGraph:', processedGraph);
            knownBookmarks.add(tweet.id); // Mark as bookmarked
            await emitBookmark(tweet.id, tweet.data, 'userscript_bookmarks_timeline', {
                expectGraphLater: true,
                allowDuplicate: true
            });
            continue;
        }

        if (status === 'processed' && !processedGraph) {
            log('⏳ Bookmark awaiting GraphQL detail:', tweet.id);
            knownBookmarks.add(tweet.id); // Mark as bookmarked
            pendingGraphTweets.add(tweet.id);
            // Ensure the detail view is fetched to obtain GraphQL
            try {
                const detailUrl = `https://x.com/i/api/graphql/3XDb5pK4h33U5iSnGKv0uQ/TweetDetail?variables=${encodeURIComponent(JSON.stringify({
                    focalTweetId: tweet.id,
                    with_rux_injections: false,
                    includePromotedContent: true,
                    withCommunity: true,
                    withQuickPromoteEligibilityTweetFields: true,
                    withBirdwatchNotes: true,
                    withVoice: true,
                    withV2Timeline: true
                }))}`;
                GM_xmlhttpRequest({
                    method: 'GET',
                    url: detailUrl,
                    headers: { 'Content-Type': 'application/json' },
                    onload: () => log('📡 Prefetched TweetDetail for', tweet.id),
                    onerror: () => log('⚠️ Failed to prefetch TweetDetail for', tweet.id)
                });
            } catch (err) {
                log('⚠️ Prefetch error for TweetDetail:', err);
            }
        } else if (processedGraph) {
            pendingGraphTweets.delete(tweet.id);
        }
        }
        } catch (err) {
            log('⚠️ Failed to sync bookmarks timeline:', err);
        }
    }

    function installFetchInterceptor() {
        const uw = typeof unsafeWindow !== 'undefined' ? unsafeWindow : window;
        const origFetch = uw.fetch;
        uw.fetch = async function (...args) {
            const resPromise = origFetch.apply(this, args);
            try {
                const [input, init] = args;
                const url = typeof input === 'string' ? input : input.url;
                const method = (init && init.method) || 'GET';
                if (/graphql/.test(url)) {
                    const text = (init && init.body) ? (typeof init.body === 'string' ? init.body : '') : '';
                    let bodyObj = null;
                    try { bodyObj = text ? JSON.parse(text) : null; } catch (_) { bodyObj = null; }
                    
                    // Debug logging - log ALL GraphQL to see what we're missing
                    const urlPath = new URL(url).pathname;
                    const queryId = urlPath.split('/').pop();
                    log(`📡 GraphQL ${method}:`, queryId.substring(0, 30), '...');
                    
                    // Check if it's TweetDetail
                    const isTweetDetailUrl = /\/graphql\/.+\/TweetDetail/.test(url);
                    const isModeratedTimelineUrl = /\/graphql\/.+\/ModeratedTimeline/.test(url);
                    
                    if (isTweetDetailUrl || isModeratedTimelineUrl) {
                        log('🎯 MATCHED TweetDetail/ModeratedTimeline pattern!', url.substring(0, 150));
                    }
                    
                    const isBookmark = bodyObj && isBookmarkMutation(url, bodyObj);
                    const isDetail = isTweetDetailUrl || isModeratedTimelineUrl;
                    const isBookmarkTimeline = isBookmarksTimelineQuery(url);

                    if (isDetail) {
                        log('🔍 TweetDetail detected (isDetail=true)');
                    }

                    if ((bodyObj && (isBookmark || isDetail)) || isDetail) {
                        const tweetId = bodyObj ? extractTweetIdFromGraphQL(bodyObj) : extractTweetIdFromUrl(url);
                        const timestamp = nowIso();
                        const payloadBase = {
                            tweet_id: tweetId,
                            tweet_data: null,
                            graphql_response: null,
                            timestamp,
                            source: isBookmark ? 'userscript_fetch' : 'userscript_fetch'
                        };
                        // Clone response to read body once available
                        resPromise.then(async (res) => {
                            try {
                                const clone = res.clone();
                                const respJson = await clone.json().catch(() => null);

                                if (respJson && isBookmarkTimeline) {
                                    await syncBookmarksFromTimeline(respJson);
                                }

                                // If this is a TweetDetail response, ALWAYS cache it
                                if (respJson && isDetail) {
                                    // Try multiple extraction methods
                                    let respTweetId = extractTweetIdFromGraphQLResponse(respJson);
                                    
                                    if (!respTweetId) {
                                        respTweetId = tweetId || extractTweetIdFromUrl(url);
                                    }
                                    
                                    // Last resort: try to get focalTweetId from URL
                                    if (!respTweetId && url.includes('focalTweetId')) {
                                        const urlParams = new URL(url).searchParams;
                                        const variables = urlParams.get('variables');
                                        if (variables) {
                                            try {
                                                const vars = JSON.parse(variables);
                                                respTweetId = vars.focalTweetId;
                                            } catch (_) {}
                                        }
                                    }
                                    
                                    const finalId = respTweetId || tweetId || extractTweetIdFromUrl(url);

                                    if (finalId) {
                                        // ALWAYS cache the response for potential future use
                                        cacheGraphQLResponse(finalId, respJson);
                                        
                                        // Check if this tweet is actually bookmarked in the GraphQL response
                                        const isBookmarked = isTweetBookmarked(respJson, finalId);
                                        
                                        if (isBookmarked) {
                                            // Process already bookmarked tweet
                                            knownBookmarks.add(finalId);
                                            
                                            const statusInfo = (await fetchBookmarkStatuses([finalId]))[finalId] || {};
                                            const needsGraph = statusInfo.status !== 'processed' || !statusInfo.processed_with_graphql;
                                            if (needsGraph) {
                                                log('✅ Processing already-bookmarked tweet with GraphQL:', finalId);
                                                const detailInfo = findTweetResultInDetail(respJson, finalId);
                                                const detailData = detailInfo ? extractTweetDataFromResult(detailInfo.result) : null;
                                                await emitBookmark(finalId, detailData, 'userscript_tweetdetail_bookmarked', {
                                                    allowDuplicate: true
                                                });
                                            }
                                            pendingGraphTweets.delete(finalId);
                                        } else if (pendingGraphTweets.has(finalId)) {
                                            // This tweet is pending processing (bookmarked elsewhere)
                                            log('📦 Processing pending bookmark with cached GraphQL:', finalId);
                                            
                                            const statusInfo = (await fetchBookmarkStatuses([finalId]))[finalId] || {};
                                            const needsGraph = statusInfo.status !== 'processed' || !statusInfo.processed_with_graphql;
                                            if (needsGraph) {
                                                log('✅ Processing pending bookmarked tweet with GraphQL:', finalId);
                                                const detailInfo = findTweetResultInDetail(respJson, finalId);
                                                const detailData = detailInfo ? extractTweetDataFromResult(detailInfo.result) : null;
                                                await emitBookmark(finalId, detailData, 'userscript_tweetdetail_pending', {
                                                    allowDuplicate: true
                                                });
                                            }
                                            pendingGraphTweets.delete(finalId);
                                        } else {
                                            log('💾 Cached TweetDetail for potential future use:', finalId);
                                        }
                                    } else {
                                        log('⚠️ Could not extract tweet ID from TweetDetail response');
                                    }
                                }
                                
                                // Bookmark mutation = user intent. Send once via emitter.
                                if (isBookmark) {
                                    const id = tweetId || extractTweetIdFromUrl(url);
                                    const isCreate = /\/CreateBookmark$/.test(url);
                                    const isDelete = /\/DeleteBookmark$/.test(url);
                                    
                                    if (isCreate) {
                                        log('🔖 CreateBookmark mutation detected for:', id);
                                        knownBookmarks.add(id); // Mark as bookmarked
                                        await emitBookmark(id, null, 'userscript_create_bookmark');
                                    } else if (isDelete) {
                                        log('🗑️ DeleteBookmark mutation detected for:', id);
                                        knownBookmarks.delete(id); // Remove from bookmarks
                                        pendingGraphTweets.delete(id);
                                        
                                        // Optionally call delete API to clean up artifacts
                                        // Uncomment the following to enable automatic cleanup:
                                        /*
                                        try {
                                            GM_xmlhttpRequest({
                                                method: 'DELETE',
                                                url: `${THOTH_API_URL}/api/bookmark/${id}`,
                                                onload: (res) => {
                                                    if (res.status >= 200 && res.status < 300) {
                                                        log('✅ Deleted artifacts for unbookmarked tweet:', id);
                                                    }
                                                },
                                                onerror: () => log('⚠️ Failed to delete artifacts for:', id)
                                            });
                                        } catch (_) {}
                                        */
                                    }
                                }
                            } catch (_) {
                                if (isBookmark) {
                                    const id = tweetId || extractTweetIdFromUrl(url);
                                    await emitBookmark(id, null, 'userscript_fetch_mutation');
                                }
                            }
                        });
                    }
                }
            } catch (e) {
                log('Interceptor error', e);
            }
            return resPromise;
        };
    }

    function installXHRInterceptor() {
        const uw = typeof unsafeWindow !== 'undefined' ? unsafeWindow : window;
        const OrigXHR = uw.XMLHttpRequest;
        function WrappedXHR() {
            const xhr = new OrigXHR();
            let info = { method: null, url: null, bodyObj: null };
            const origOpen = xhr.open;
            xhr.open = function(method, url) {
                info.method = method;
                info.url = url;
                return origOpen.apply(xhr, arguments);
            };
            const origSend = xhr.send;
            xhr.send = function(body) {
                try {
                    if (body && typeof body === 'string') {
                        try { info.bodyObj = JSON.parse(body); } catch (_) { info.bodyObj = null; }
                    }
                } catch (_) {}
        xhr.addEventListener('loadend', async function() {
            try {
                const url = info.url || '';
                if (!/graphql/.test(url)) return;
                        
                        // Debug logging for XHR
                        const urlPath = new URL(url).pathname;
                        const queryId = urlPath.split('/').pop();
                        log(`📡 XHR ${info.method}:`, queryId.substring(0, 30), '...');
                        
                        const isBookmark = info.bodyObj && isBookmarkMutation(url, info.bodyObj);
                        const isDetail = (info.bodyObj && isTweetDetailQuery(url, info.bodyObj)) || (!info.bodyObj && isTweetDetailQuery(url, {}));
                        const isBookmarkTimeline = isBookmarksTimelineQuery(url);
                        if (!(isBookmark || isDetail || isBookmarkTimeline)) return;
                        const tweetId = info.bodyObj ? extractTweetIdFromGraphQL(info.bodyObj) : extractTweetIdFromUrl(url);
                        const timestamp = nowIso();
                        let respJson = null;
                        try { respJson = JSON.parse(xhr.responseText); } catch (_) { respJson = null; }
                        
                        if (respJson && isBookmarkTimeline) {
                            await syncBookmarksFromTimeline(respJson);
                        }

                        // TweetDetail: ALWAYS cache, then check if needs processing
                        if (respJson && isDetail) {
                            const respTweetId = extractTweetIdFromGraphQLResponse(respJson) ||
                                              tweetId || extractTweetIdFromUrl(url);
                            const finalId = respTweetId || tweetId || extractTweetIdFromUrl(url);
                            if (finalId) {
                                // ALWAYS cache the response
                                cacheGraphQLResponse(finalId, respJson);
                                
                                // Check if this tweet is actually bookmarked
                                const isBookmarked = isTweetBookmarked(respJson, finalId);
                                
                                if (isBookmarked) {
                                    // Process already bookmarked tweet
                                    knownBookmarks.add(finalId);
                                    
                                    const statusInfo = (await fetchBookmarkStatuses([finalId]))[finalId] || {};
                                    const needsGraph = statusInfo.status !== 'processed' || !statusInfo.processed_with_graphql;
                                    if (needsGraph) {
                                        log('✅ Processing already-bookmarked tweet with GraphQL (XHR):', finalId);
                                        const detailInfo = findTweetResultInDetail(respJson, finalId);
                                        const detailData = detailInfo ? extractTweetDataFromResult(detailInfo.result) : null;
                                        await emitBookmark(finalId, detailData, 'userscript_tweetdetail_bookmarked_xhr', {
                                            allowDuplicate: true
                                        });
                                    }
                                    pendingGraphTweets.delete(finalId);
                                } else if (pendingGraphTweets.has(finalId)) {
                                    // This tweet is pending processing
                                    log('📦 Processing pending bookmark with cached GraphQL (XHR):', finalId);
                                    
                                    const statusInfo = (await fetchBookmarkStatuses([finalId]))[finalId] || {};
                                    const needsGraph = statusInfo.status !== 'processed' || !statusInfo.processed_with_graphql;
                                    if (needsGraph) {
                                        log('✅ Processing pending bookmarked tweet with GraphQL (XHR):', finalId);
                                        const detailInfo = findTweetResultInDetail(respJson, finalId);
                                        const detailData = detailInfo ? extractTweetDataFromResult(detailInfo.result) : null;
                                        await emitBookmark(finalId, detailData, 'userscript_tweetdetail_pending_xhr', {
                                            allowDuplicate: true
                                        });
                                    }
                                    pendingGraphTweets.delete(finalId);
                                } else {
                                    log('💾 Cached TweetDetail for potential future use (XHR):', finalId);
                                }
                            }
                        }

                        // Bookmark mutation = user intent. Send once via emitter.
                        if (isBookmark) {
                            const id = tweetId || extractTweetIdFromUrl(url);
                            const isCreate = /\/CreateBookmark$/.test(url);
                            const isDelete = /\/DeleteBookmark$/.test(url);
                            
                            if (isCreate) {
                                log('🔖 CreateBookmark mutation detected for (XHR):', id);
                                knownBookmarks.add(id); // Mark as bookmarked
                                const statusInfo = (await fetchBookmarkStatuses([id]))[id] || {};
                                const processedGraph = !!statusInfo.processed_with_graphql;
                                await emitBookmark(id, null, 'userscript_create_bookmark_xhr', {
                                    expectGraphLater: !processedGraph
                                });
                            } else if (isDelete) {
                                log('🗑️ DeleteBookmark mutation detected for (XHR):', id);
                                knownBookmarks.delete(id); // Remove from bookmarks
                                pendingGraphTweets.delete(id);
                                
                                // Optionally call delete API to clean up artifacts
                                // Uncomment the following to enable automatic cleanup:
                                
                                try {
                                    GM_xmlhttpRequest({
                                        method: 'DELETE',
                                        url: `${THOTH_API_URL}/api/bookmark/${id}`,
                                        onload: (res) => {
                                            if (res.status >= 200 && res.status < 300) {
                                                log('✅ Deleted artifacts for unbookmarked tweet (XHR):', id);
                                            }
                                        },
                                        onerror: () => log('⚠️ Failed to delete artifacts for (XHR):', id)
                                    });
                                } catch (_) {}
                               
                            }
                        }
                } catch (e) { /* ignore */ }
            });
            return origSend.apply(xhr, arguments);
        };
        return xhr;
        }
        uw.XMLHttpRequest = WrappedXHR;
    }

    async function sendOrQueue(payload) {
        const base = getProcessedBaseSet();
        const full = getProcessedFullSet();
        if (!payload.tweet_id) return;
        const hasGraph = !!payload.graphql_response;
        const forced = !!payload.force;
        if (forced) {
            log('🔁 Forcing resend for', payload.tweet_id, 'hasGraph:', hasGraph);
        }
        if (hasGraph) {
            if (!forced && full.has(payload.tweet_id)) return;
        } else {
            if (!forced && (base.has(payload.tweet_id) || full.has(payload.tweet_id))) return;
        }
        const healthy = await apiHealth();
        if (!healthy) { enqueue(payload); return; }
        try {
            await postBookmark(payload);
        } catch (_) {
            enqueue(payload);
            return;
        }
        if (hasGraph) {
            full.add(payload.tweet_id);
            saveProcessedFullSet(full);
        } else {
            base.add(payload.tweet_id);
            saveProcessedBaseSet(base);
        }
        GM_notification({ title: 'Thoth', text: 'Captured ' + payload.tweet_id, timeout: 1500 });
    }

    // Removed DOM monitoring - we rely on GraphQL mutations instead

    // One-shot guard to avoid double-sends when both DOM and mutation fire
    const pendingSends = new Map(); // tweet_id -> timeoutId

    async function emitBookmark(tweetId, tweetData, sourceHint, options = {}) {
        if (!tweetId) return;
        const { allowDuplicate = false, expectGraphLater = false } = options;

        if (!allowDuplicate && pendingSends.has(tweetId)) return;
        // Small window to coalesce DOM + mutation for same click
        const tid = setTimeout(() => pendingSends.delete(tweetId), 2000);
        pendingSends.set(tweetId, tid);

        const cachedGraphQL = getCachedGraphQLResponse(tweetId);
        if (!cachedGraphQL) {
            log('⚠️ No cached GraphQL for tweet:', tweetId);
            log('📊 Current cache size:', graphqlCache.size, 'entries');
            if (expectGraphLater) {
                pendingGraphTweets.add(tweetId);
            }
        } else {
            pendingGraphTweets.delete(tweetId);
        }

        const payload = {
            tweet_id: tweetId,
            tweet_data: tweetData || null,
            graphql_response: cachedGraphQL || null,
            timestamp: nowIso(),
            source: cachedGraphQL ? `${sourceHint}_cached` : `${sourceHint}_minimal`
        };
        if (allowDuplicate && cachedGraphQL) {
            payload.force = true;
        }
        await sendOrQueue(payload);
    }

    // Periodic queue flush
    setInterval(flushQueue, 5000);

    // Kickoff
    log('🚀 Thoth Live Capture initializing...');
    
    // Install interceptors immediately
    installFetchInterceptor();
    log('✅ Fetch interceptor installed');
    
    installXHRInterceptor();
    log('✅ XHR interceptor installed');
    
    log('🚀 Thoth Live Capture ready - watching for CreateBookmark mutations...');
})();
