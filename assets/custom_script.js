// assets/custom_script.js

// Ensure the global clientside object and its 'clientside' namespace exist
if (!window.dash_clientside) { window.dash_clientside = {}; }
if (!window.dash_clientside.clientside) {
    window.dash_clientside.clientside = {};
}

// Helper function to safely return no_update (good for debugging)
function safe_no_update(functionName) {
    if (typeof window.dash_clientside.no_update === 'undefined') {
        console.error(`CRITICAL ERROR in [${functionName}]: window.dash_clientside.no_update is undefined! Dash clientside callbacks will likely fail.`);
        return; // Return undefined if no_update is broken, Dash might handle it or error further
    }
    return window.dash_clientside.no_update;
}

// Assign all functions as properties of window.dash_clientside.clientside
Object.assign(window.dash_clientside.clientside, {
    // === Timezone Detection Function ===
    getTimezone: function(pathname_trigger_value) {
        const funcName = 'getTimezone';
        try {
            const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
            console.log(`CLIENTSIDE JS: [${funcName}] called for path:`, pathname_trigger_value, "Detected TZ:", timezone);
            return timezone;
        } catch (e) {
            console.error(`[JS ${funcName}] Error:`, e);
            return safe_no_update(funcName); // Or return a default/error indicator if appropriate
        }
    },

    // === Your Existing Properties and Functions ===
    plotlyReactedMap: {}, // Stores the uirevision of the layout last used by Plotly.react
    plotlyReactedMapDataKey: {}, // Stores a key representing the data content last reacted to
    _trackMapResizedRecently: false,
    _resizeTimeoutId: null,
    _trackMapResizeObserver: null,
    _resizeMapTimeoutId: null,
    _resizeRecentlyTimeoutId: null,

    animateCarMarkers: function (newCarDataFromStore, trackMapVersion, existingFigureFromState, graphDivId, updateIntervalDuration) {
        const funcName = 'animateCarMarkers';
        if (typeof Plotly === 'undefined' || !Plotly) {
            console.warn(`[JS ${funcName}] Plotly object not found.`);
            return safe_no_update(funcName);
        }
        const gd = document.getElementById(graphDivId);
        if (!gd) {
            console.warn(`[JS ${funcName}] Graph div not found:`, graphDivId);
            return safe_no_update(funcName);
        }
        // It's good practice to also check if gd.data and gd.layout exist if not doing Plotly.react immediately
        // This indicates Plotly has initialized the div.
        // if (!gd.data || !gd.layout) {
        //     console.warn(`[JS ${funcName}] Plotly graph data/layout not initialized for div '${graphDivId}'.`);
        // }

        let figureToProcess = existingFigureFromState;

        if (!figureToProcess || !figureToProcess.layout || !figureToProcess.data) {
            console.warn(`[JS ${funcName}] existingFigureFromState is null/undefined or lacks layout/data.`);
            if (trackMapVersion && window.dash_clientside.clientside.plotlyReactedMap[graphDivId] && figureToProcess && figureToProcess.layout && figureToProcess.layout.uirevision && window.dash_clientside.clientside.plotlyReactedMap[graphDivId] !== figureToProcess.layout.uirevision) {
                delete window.dash_clientside.clientside.plotlyReactedMap[graphDivId];
                delete window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId];
            }
            return safe_no_update(funcName);
        }
        if (!Array.isArray(figureToProcess.data)) {
            console.warn(`[JS ${funcName}] figureToProcess.data is not an array. Figure:`, JSON.stringify(figureToProcess));
            figureToProcess.data = [];
        }

        const currentLayoutUiRevisionFromPython = figureToProcess.layout.uirevision;
        const lastReactedLayoutUiRevision = window.dash_clientside.clientside.plotlyReactedMap[graphDivId];
        let reactedInThisCall = false;

        let pythonFigureDataKey = "nodata";
        if (figureToProcess.data.length > 0) {
            try {
                pythonFigureDataKey = figureToProcess.data.map(t =>
                    `${t.name || 'u'}_${t.visible === false ? 'f' : 't'}_${(t.x && Array.isArray(t.x) && t.x.length > 0 && t.x[0] !== null) ? 'd' : 'e'}`
                ).join('-');
            } catch (e) {
                console.error(`[JS ${funcName}] Error creating pythonFigureDataKey:`, e, "Trace data:", figureToProcess.data);
                pythonFigureDataKey = `error-${new Date().getTime()}`;
            }
        }
        const lastReactedDataKey = window.dash_clientside.clientside.plotlyReactedMapDataKey && window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId];

        if ( (trackMapVersion && currentLayoutUiRevisionFromPython && (lastReactedLayoutUiRevision !== currentLayoutUiRevisionFromPython)) ||
             (pythonFigureDataKey !== lastReactedDataKey) ) {
            const dataForReact = Array.isArray(figureToProcess.data) ? figureToProcess.data : [];
            console.log(`[JS ${funcName}] Calling Plotly.react. Reason: LayoutUirevChanged: ${lastReactedLayoutUiRevision !== currentLayoutUiRevisionFromPython}, DataKeyChanged: ${pythonFigureDataKey !== lastReactedDataKey}.`);
            try {
                Plotly.react(gd, dataForReact, figureToProcess.layout, figureToProcess.config || {});
                window.dash_clientside.clientside.plotlyReactedMap[graphDivId] = currentLayoutUiRevisionFromPython;
                window.dash_clientside.clientside.plotlyReactedMapDataKey = window.dash_clientside.clientside.plotlyReactedMapDataKey || {};
                window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId] = pythonFigureDataKey;
                reactedInThisCall = true;
            } catch (e) {
                console.error(`[JS ${funcName}] Error during Plotly.react:`, e, 'Figure:', JSON.stringify(figureToProcess));
                return safe_no_update(funcName);
            }
        }

        const newCarPositions = newCarDataFromStore ? newCarDataFromStore.cars : null;
        const selectedDriverUID = newCarDataFromStore ? newCarDataFromStore.selected_driver : null;
        const storeStatus = newCarDataFromStore ? newCarDataFromStore.status : null;

        if (reactedInThisCall && (!newCarPositions || Object.keys(newCarPositions).length === 0 || storeStatus !== 'active')) {
            return safe_no_update(funcName);
        }
        if (storeStatus === 'reset_map_display' || !newCarPositions || Object.keys(newCarPositions).length === 0 || storeStatus !== 'active') {
            return safe_no_update(funcName);
        }

        const traces_in_python_figure = figureToProcess.data;
        if (!traces_in_python_figure || !Array.isArray(traces_in_python_figure) || traces_in_python_figure.length === 0) {
            console.warn(`[JS ${funcName}] Animation path: figureToProcess.data (from State) is empty. Cannot animate cars.`);
            return safe_no_update(funcName);
        }

        let uidToTraceIndex = {};
        traces_in_python_figure.forEach((trace, index) => {
            if (trace && typeof trace.uid === 'string' && trace.uid.trim() !== "") {
                uidToTraceIndex[trace.uid] = index;
            }
        });

        let traceIndicesToUpdateInPlotly = [];
        let restyleUpdate = {
            x: [], y: [], text: [],
            'marker.color': [], 'marker.opacity': [], 'marker.size': [], 'marker.line.width': [], 'marker.line.color': [],
            'textfont.color': []
        };
        let animateDataPayload = [];

        const SELECTED_MARKER_SIZE = 12;
        const SELECTED_MARKER_LINE_WIDTH = 2;
        const SELECTED_MARKER_LINE_COLOR = 'white';
        const DEFAULT_MARKER_SIZE = 8;
        const DEFAULT_MARKER_LINE_WIDTH = 1;
        const DEFAULT_MARKER_LINE_COLOR = 'Black';

        for (const carUID in newCarPositions) {
            const carInfo = newCarPositions[carUID];
            const originalTraceIndex = uidToTraceIndex[carUID];
            if (originalTraceIndex !== undefined && gd.data && originalTraceIndex < gd.data.length) {
                if (!traceIndicesToUpdateInPlotly.includes(originalTraceIndex)) {
                    traceIndicesToUpdateInPlotly.push(originalTraceIndex);
                }
                let k_idx = traceIndicesToUpdateInPlotly.indexOf(originalTraceIndex);
                Object.keys(restyleUpdate).forEach(key => { while(restyleUpdate[key].length <= k_idx) restyleUpdate[key].push(undefined); });
                while(animateDataPayload.length <= k_idx) animateDataPayload.push({});

                const tla = (typeof carInfo.tla === 'string' && carInfo.tla.trim() !== '') ? carInfo.tla : carUID.toString();
                const markerColor = (typeof carInfo.color === 'string' && carInfo.color.startsWith('#')) ? carInfo.color : '#808080';
                const carStatus = (typeof carInfo.status === 'string') ? carInfo.status.toLowerCase() : "";
                const isDimmed = carStatus.includes('retired') || carStatus.includes('pit') || carStatus.includes('stopped');
                const markerOpacity = isDimmed ? 0.3 : 1.0;
                const textFontColor = `rgba(255, 255, 255, ${isDimmed ? 0.35 : 1.0})`;
                const isSelected = (selectedDriverUID === carUID);

                restyleUpdate.x[k_idx] = (typeof carInfo.x === 'number' ? [carInfo.x] : [null]);
                restyleUpdate.y[k_idx] = (typeof carInfo.y === 'number' ? [carInfo.y] : [null]);
                restyleUpdate.text[k_idx] = ([tla]);
                restyleUpdate['marker.color'][k_idx] = (markerColor);
                restyleUpdate['marker.opacity'][k_idx] = (markerOpacity);
                restyleUpdate['textfont.color'][k_idx] = (textFontColor);
                restyleUpdate['marker.size'][k_idx] = (isSelected ? SELECTED_MARKER_SIZE : DEFAULT_MARKER_SIZE);
                restyleUpdate['marker.line.width'][k_idx] = (isSelected ? SELECTED_MARKER_LINE_WIDTH : DEFAULT_MARKER_LINE_WIDTH);
                restyleUpdate['marker.line.color'][k_idx] = (isSelected ? SELECTED_MARKER_LINE_COLOR : DEFAULT_MARKER_LINE_COLOR);

                animateDataPayload[k_idx] = {
                    x: restyleUpdate.x[k_idx], y: restyleUpdate.y[k_idx], text: restyleUpdate.text[k_idx],
                    marker: {
                        color: markerColor, opacity: markerOpacity, size: restyleUpdate['marker.size'][k_idx],
                        line: { width: restyleUpdate['marker.line.width'][k_idx], color: restyleUpdate['marker.line.color'][k_idx] }
                    },
                    textfont: { color: textFontColor }
                };
            }
        }

        traces_in_python_figure.forEach((trace, originalTraceIndex) => {
            if (trace && typeof trace.uid === 'string' && trace.uid.trim() !== "") {
                if (!newCarPositions || !newCarPositions[trace.uid]) {
                    if (gd.data && originalTraceIndex < gd.data.length) {
                        if (!traceIndicesToUpdateInPlotly.includes(originalTraceIndex)) {
                            traceIndicesToUpdateInPlotly.push(originalTraceIndex);
                        }
                        let k_idx = traceIndicesToUpdateInPlotly.indexOf(originalTraceIndex);
                        Object.keys(restyleUpdate).forEach(key => { while(restyleUpdate[key].length <= k_idx) restyleUpdate[key].push(undefined); });
                        while(animateDataPayload.length <= k_idx) animateDataPayload.push({});

                        restyleUpdate.x[k_idx] = ([null]); /* ... other properties to hide/reset trace ... */
                        restyleUpdate.y[k_idx] = ([null]);
                        restyleUpdate.text[k_idx] = (['']); 
                        restyleUpdate['marker.color'][k_idx] = ('#333333'); 
                        restyleUpdate['marker.opacity'][k_idx] = (0.1);
                        restyleUpdate['marker.size'][k_idx] = (DEFAULT_MARKER_SIZE); 
                        restyleUpdate['marker.line.width'][k_idx] = (DEFAULT_MARKER_LINE_WIDTH);
                        restyleUpdate['marker.line.color'][k_idx] = (DEFAULT_MARKER_LINE_COLOR);
                        restyleUpdate['textfont.color'][k_idx] = ('rgba(255,255,255,0.1)');

                        animateDataPayload[k_idx] = {
                            x: [null], y: [null], text: [''],
                            marker: { color: '#333333', opacity: 0.1, size: DEFAULT_MARKER_SIZE,
                                      line: { width: DEFAULT_MARKER_LINE_WIDTH, color: DEFAULT_MARKER_LINE_COLOR } },
                            textfont: { color: 'rgba(255,255,255,0.1)' }
                        };
                    }
                }
            }
        });

        if (traceIndicesToUpdateInPlotly.length === 0) {
            return safe_no_update(funcName);
        }

        const DURATION_THRESHOLD_MS = 600; let animationDuration = 0;
        if (window.dash_clientside.clientside._trackMapResizedRecently) {
            animationDuration = 0;
            if(window.dash_clientside.clientside._resizeTimeoutId) { clearTimeout(window.dash_clientside.clientside._resizeTimeoutId); }
            window.dash_clientside.clientside._resizeTimeoutId = setTimeout(() => { window.dash_clientside.clientside._trackMapResizedRecently = false; }, 500);
        } else if (updateIntervalDuration && updateIntervalDuration > DURATION_THRESHOLD_MS) {
            animationDuration = Math.max(50, updateIntervalDuration * 0.90);
        } else if (updateIntervalDuration) {
            animationDuration = Math.min(50, updateIntervalDuration * 0.5);
            if (animationDuration < 20) animationDuration = 0;
        }

        try {
            if (animationDuration > 0 && Plotly.animate) {
                Plotly.animate(gd, { data: animateDataPayload, traces: traceIndicesToUpdateInPlotly }, { transition: { duration: animationDuration, easing: 'linear' }, frame: { duration: animationDuration, redraw: false } });
            } else if (Plotly.restyle) {
                Plotly.restyle(gd, restyleUpdate, traceIndicesToUpdateInPlotly);
            }
        } catch (e) { console.error(`[JS ${funcName}] Error during Plotly.animate/restyle:`, e); }
        
        return safe_no_update(funcName);
    },

    setupTrackMapResizeListener: function(figure) { // 'figure' is a dummy input to trigger this
        const funcName = 'setupTrackMapResizeListener';
        if (typeof Plotly === 'undefined' || !Plotly) {
            console.warn(`[JS ${funcName}] Plotly object not found.`);
            return safe_no_update(funcName);
        }
        const graphDivId = 'track-map-graph';
        const graphDiv = document.getElementById(graphDivId);

        if (graphDiv && !window.dash_clientside.clientside._trackMapResizeObserver) {
            try {
                const resizeObserver = new ResizeObserver(entries => {
                    if (window.dash_clientside.clientside._resizeMapTimeoutId) {
                        clearTimeout(window.dash_clientside.clientside._resizeMapTimeoutId);
                    }
                    window.dash_clientside.clientside._resizeMapTimeoutId = setTimeout(() => {
                        const currentGraphDiv = document.getElementById(graphDivId);
                        if (currentGraphDiv && currentGraphDiv.offsetParent !== null &&
                            typeof Plotly.Plots !== 'undefined' && Plotly.Plots.resize) {
                            console.log(`[JS ${funcName}] Debounced: Calling Plotly.Plots.resize on ${graphDivId}`);
                            Plotly.Plots.resize(currentGraphDiv);

                            window.dash_clientside.clientside._trackMapResizedRecently = true;
                            if(window.dash_clientside.clientside._resizeRecentlyTimeoutId) {
                                clearTimeout(window.dash_clientside.clientside._resizeRecentlyTimeoutId);
                            }
                            window.dash_clientside.clientside._resizeRecentlyTimeoutId = setTimeout(() => {
                                window.dash_clientside.clientside._trackMapResizedRecently = false;
                            }, 550); // Duration slightly longer than debounce + common transitions
                        }
                    }, 350); // Debounce delay (e.g., 300ms for sidebar + 50ms buffer)
                });

                const wrapperDiv = graphDiv.parentElement;
                if (wrapperDiv) {
                    resizeObserver.observe(wrapperDiv);
                    window.dash_clientside.clientside._trackMapResizeObserver = resizeObserver;
                    console.log(`[JS ${funcName}] ResizeObserver attached to wrapper of ${graphDivId}`);
                } else {
                     console.warn(`[JS ${funcName}] Parent wrapper of ${graphDivId} not found for ResizeObserver.`);
                }
            } catch (e) { console.error(`[JS ${funcName}] Error setting up ResizeObserver:`, e); }
        }
        return safe_no_update(funcName);
    },

    setupClickToFocusListener: function(figure) {
        const funcName = 'setupClickToFocusListener';
        if (typeof Plotly === 'undefined' || !Plotly) {
            console.warn(`[JS ${funcName}] Plotly object not found.`);
            return safe_no_update(funcName);
        }
        const graphDivId = 'track-map-graph';
        const gd = document.getElementById(graphDivId);
        const clickDataHolderId = 'js-click-data-holder';

        if (gd) {
            if (gd.hasOwnProperty('_hasF1ClickFocusListenerPolling')) {
                return safe_no_update(funcName);
            }
            gd.on('plotly_click', function(data) {
                let clickEventDataToSend = null;
                if (data.points.length > 0) {
                    const point = data.points[0];
                    if (point.data && typeof point.data.uid === 'string' && point.data.uid.trim() !== "") {
                        clickEventDataToSend = { carNumber: point.data.uid, ts: new Date().getTime() };
                    }
                }
                if (!clickEventDataToSend) { // If not a car click, signal to clear or no specific car
                    clickEventDataToSend = { carNumber: null, ts: new Date().getTime() };
                }
                const dataHolderDiv = document.getElementById(clickDataHolderId);
                if (dataHolderDiv) {
                    dataHolderDiv.textContent = JSON.stringify(clickEventDataToSend);
                } else {
                    console.warn(`[JS ${funcName}] js-click-data-holder div not found.`);
                }
            });
            gd._hasF1ClickFocusListenerPolling = true;
        } else {
            console.warn(`[JS ${funcName}] Graph div '${graphDivId}' not found for click listener.`);
        }
        return safe_no_update(funcName);
    },

    pollClickDataAndUpdateStore: function(n_intervals, _ignored_clickDataFromDivChildren_via_state) {
        const funcName = 'pollClickDataAndUpdateStore';
        const clickDataHolderId = 'js-click-data-holder';
        const dataHolderDiv = document.getElementById(clickDataHolderId);
        let currentClickDataInDiv = null;

        if (dataHolderDiv) {
            currentClickDataInDiv = dataHolderDiv.textContent;
        }

        if (typeof window.dash_clientside.clientside._lastClickDataSentToStore === 'undefined') {
            window.dash_clientside.clientside._lastClickDataSentToStore = null;
        }

        if (currentClickDataInDiv === null || typeof currentClickDataInDiv === 'undefined' || currentClickDataInDiv.trim() === "") {
            if (window.dash_clientside.clientside._lastClickDataSentToStore !== null && window.dash_clientside.clientside._lastClickDataSentToStore !== "") {
                window.dash_clientside.clientside._lastClickDataSentToStore = "";
                return ""; 
            }
            return safe_no_update(funcName);
        }

        if (currentClickDataInDiv !== window.dash_clientside.clientside._lastClickDataSentToStore) {
            window.dash_clientside.clientside._lastClickDataSentToStore = currentClickDataInDiv;
            return currentClickDataInDiv;
        } else {
            return safe_no_update(funcName);
        }
    }
}); // End of Object.assign for window.dash_clientside.clientside