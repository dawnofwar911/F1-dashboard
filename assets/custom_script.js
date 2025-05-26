// assets/custom_script.js

if (!window.dash_clientside) { window.dash_clientside = {}; }

window.dash_clientside.clientside = {
    plotlyReactedMap: {}, // Stores the uirevision of the layout last used by Plotly.react
    plotlyReactedMapDataKey: {}, // Stores a key representing the data content last reacted to
    _trackMapResizedRecently: false,
    _resizeTimeoutId: null,
    _trackMapResizeObserver: null,

    animateCarMarkers: function (newCarDataFromStore, trackMapVersion, existingFigureFromState, graphDivId, updateIntervalDuration) {
        const gd = document.getElementById(graphDivId);
        if (!gd) {
            console.warn("[JS animateCarMarkers] Graph div not found:", graphDivId);
            return window.dash_clientside.no_update;
        }

        let figureToProcess = existingFigureFromState;

        if (!figureToProcess || !figureToProcess.layout || !figureToProcess.data) {
            console.warn("[JS animateCarMarkers] existingFigureFromState (figureToProcess) is null/undefined or lacks layout/data. Waiting for a valid figure.");
            if (trackMapVersion && window.dash_clientside.clientside.plotlyReactedMap[graphDivId] && figureToProcess && figureToProcess.layout && figureToProcess.layout.uirevision && window.dash_clientside.clientside.plotlyReactedMap[graphDivId] !== figureToProcess.layout.uirevision) {
                 console.log("[JS animateCarMarkers] uirevision changed but figureToProcess is incomplete. Clearing reactedMap uirevision to force react on next valid figure.");
                 delete window.dash_clientside.clientside.plotlyReactedMap[graphDivId];
                 delete window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId];
            }
            return window.dash_clientside.no_update;
        }
        if (!Array.isArray(figureToProcess.data)) {
            console.warn("[JS animateCarMarkers] figureToProcess.data is not an array. Defaulting to empty. Figure:", JSON.stringify(figureToProcess));
            figureToProcess.data = [];
        }

        const currentLayoutUiRevisionFromPython = figureToProcess.layout.uirevision;
        const lastReactedLayoutUiRevision = window.dash_clientside.clientside.plotlyReactedMap[graphDivId];
        let reactedInThisCall = false;

        let pythonFigureDataKey = "nodata";
        if (figureToProcess.data.length > 0) {
            pythonFigureDataKey = figureToProcess.data.map(t =>
                `${t.name || 'u'}_${t.visible === false ? 'f' : 't'}_${(t.x && t.x.length > 0 && t.x[0] !== null) ? 'd' : 'e'}`
            ).join('-');
        }
        const lastReactedDataKey = window.dash_clientside.clientside.plotlyReactedMapDataKey && window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId];

        if ( (trackMapVersion && currentLayoutUiRevisionFromPython && (lastReactedLayoutUiRevision !== currentLayoutUiRevisionFromPython)) ||
             (pythonFigureDataKey !== lastReactedDataKey)
           ) {
            const dataForReact = Array.isArray(figureToProcess.data) ? figureToProcess.data : [];
            console.log(`[JS animateCarMarkers] Call Plotly.react. Reason: LayoutUirevChanged: ${lastReactedLayoutUiRevision !== currentLayoutUiRevisionFromPython}, DataKeyChanged: ${pythonFigureDataKey !== lastReactedDataKey}. PythonLayoutUirev: ${currentLayoutUiRevisionFromPython}, PythonDataKey: ${pythonFigureDataKey}`);
            // console.log("[JS React Path] figureToProcess.data (from State) before Plotly.react:",
            //     dataForReact.map(t => `${t.name || 'Unnamed'}(UID: ${t.uid || 'NoUID'}, Visible: ${t.visible === undefined ? 'N/A' : t.visible})`)
            // );

            try {
                Plotly.react(gd, dataForReact, figureToProcess.layout, figureToProcess.config || {});
                window.dash_clientside.clientside.plotlyReactedMap[graphDivId] = currentLayoutUiRevisionFromPython;
                window.dash_clientside.clientside.plotlyReactedMapDataKey = window.dash_clientside.clientside.plotlyReactedMapDataKey || {};
                window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId] = pythonFigureDataKey;
                reactedInThisCall = true;
                // console.log(`[JS animateCarMarkers] Plotly.react successful.`);
            } catch (e) {
                console.error('[JS animateCarMarkers] Error during Plotly.react:', e, 'Figure (stringified):', JSON.stringify(figureToProcess));
                return window.dash_clientside.no_update;
            }
        }

        // Extract car data and selected driver from the store's payload
        const newCarPositions = newCarDataFromStore ? newCarDataFromStore.cars : null;
        const selectedDriverUID = newCarDataFromStore ? newCarDataFromStore.selected_driver : null; // UID of selected driver
        const storeStatus = newCarDataFromStore ? newCarDataFromStore.status : null;


        if (reactedInThisCall && (!newCarPositions || Object.keys(newCarPositions).length === 0 || storeStatus !== 'active')) {
            // console.log("[JS animateCarMarkers] Just called Plotly.react, no immediate car data for animation or store not active. Returning.");
            return window.dash_clientside.no_update;
        }

        if (storeStatus === 'reset_map_display') {
            // console.log("[JS animateCarMarkers] Received 'reset_map_display' signal.");
            return window.dash_clientside.no_update;
        }

        if (!newCarPositions || Object.keys(newCarPositions).length === 0 || storeStatus !== 'active') {
            // console.log("[JS animateCarMarkers] No car positions or store not active. No animation.");
            return window.dash_clientside.no_update;
        }


        const traces_in_python_figure = figureToProcess.data;
        if (!traces_in_python_figure || !Array.isArray(traces_in_python_figure) || traces_in_python_figure.length === 0) {
            console.warn("[JS animateCarMarkers] Animation path: figureToProcess.data (from State) is empty. Cannot animate cars.");
            return window.dash_clientside.no_update;
        }
        // console.log("[JS Animate/Restyle Path] Figure from Python (figureToProcess.data) for animation logic:",
        //     traces_in_python_figure.map(t => `${t.name || 'Unnamed'}(UID: ${t.uid || 'NoUID'}, Visible: ${t.visible === undefined ? 'N/A' : t.visible})`)
        // );


        let uidToTraceIndex = {};
        traces_in_python_figure.forEach((trace, index) => {
            if (trace && typeof trace.uid === 'string' && trace.uid.trim() !== "") {
                uidToTraceIndex[trace.uid] = index;
            }
        });

        let traceIndicesToUpdateInPlotly = [];
        // Ensure all potential keys for restyleUpdate are initialized as arrays
        let restyleUpdate = {
            x: [], y: [], text: [],
            'marker.color': [], 'marker.opacity': [], 'marker.size': [], 'marker.line.width': [], 'marker.line.color': [],
            'textfont.color': []
        };
        let animateDataPayload = [];

        // Define styles for selected driver
        const SELECTED_MARKER_SIZE = 12; // Larger size for selected driver
        const SELECTED_MARKER_LINE_WIDTH = 2;
        const SELECTED_MARKER_LINE_COLOR = 'white'; // e.g., yellow or white outline

        const DEFAULT_MARKER_SIZE = 8; // From your Python config.CAR_MARKER_SIZE
        const DEFAULT_MARKER_LINE_WIDTH = 1;
        const DEFAULT_MARKER_LINE_COLOR = 'Black';


        for (const carUID in newCarPositions) { // Iterate through cars in the 'cars' object
            // carUID here is the racing number string
            const carInfo = newCarPositions[carUID];
            const originalTraceIndex = uidToTraceIndex[carUID]; // carUID should be a string here

            if (originalTraceIndex !== undefined && gd.data && originalTraceIndex < gd.data.length) {
                if (!traceIndicesToUpdateInPlotly.includes(originalTraceIndex)) {
                    traceIndicesToUpdateInPlotly.push(originalTraceIndex);
                }
                let k_idx = traceIndicesToUpdateInPlotly.indexOf(originalTraceIndex);

                // Ensure arrays in restyleUpdate and animateDataPayload are long enough
                Object.keys(restyleUpdate).forEach(key => { while(restyleUpdate[key].length <= k_idx) restyleUpdate[key].push(undefined); });
                while(animateDataPayload.length <= k_idx) animateDataPayload.push({});


                const tla = (typeof carInfo.tla === 'string' && carInfo.tla.trim() !== '') ? carInfo.tla : carUID.toString();
                const markerColor = (typeof carInfo.color === 'string' && carInfo.color.startsWith('#')) ? carInfo.color : '#808080';
                const carStatus = (typeof carInfo.status === 'string') ? carInfo.status.toLowerCase() : "";
                const isDimmed = carStatus.includes('retired') || carStatus.includes('pit') || carStatus.includes('stopped');
                const markerOpacity = isDimmed ? 0.3 : 1.0;
                const textFontColor = `rgba(255, 255, 255, ${isDimmed ? 0.35 : 1.0})`;

                // Check if this car is the selected one
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
                    x: restyleUpdate.x[k_idx],
                    y: restyleUpdate.y[k_idx],
                    text: restyleUpdate.text[k_idx],
                    marker: {
                        color: markerColor,
                        opacity: markerOpacity,
                        size: restyleUpdate['marker.size'][k_idx],
                        line: {
                            width: restyleUpdate['marker.line.width'][k_idx],
                            color: restyleUpdate['marker.line.color'][k_idx]
                        }
                    },
                    textfont: { color: textFontColor }
                };
            }
        }

        // Handle traces that are in the figure but not in the newCarPositions (e.g. cars that left the session)
        traces_in_python_figure.forEach((trace, originalTraceIndex) => {
            if (trace && typeof trace.uid === 'string' && trace.uid.trim() !== "") {
                // trace.uid here is the racing number string
                if (!newCarPositions || !newCarPositions[trace.uid]) { // If car not in current update
                    if (gd.data && originalTraceIndex < gd.data.length) {
                        if (!traceIndicesToUpdateInPlotly.includes(originalTraceIndex)) {
                            traceIndicesToUpdateInPlotly.push(originalTraceIndex);
                        }
                        let k_idx = traceIndicesToUpdateInPlotly.indexOf(originalTraceIndex);
                        Object.keys(restyleUpdate).forEach(key => { while(restyleUpdate[key].length <= k_idx) restyleUpdate[key].push(undefined); });
                        while(animateDataPayload.length <= k_idx) animateDataPayload.push({});

                        restyleUpdate.x[k_idx] = ([null]);
                        restyleUpdate.y[k_idx] = ([null]);
                        restyleUpdate.text[k_idx] = (['']); // Empty text
                        restyleUpdate['marker.color'][k_idx] = ('#333333'); // Dim color
                        restyleUpdate['marker.opacity'][k_idx] = (0.1);
                        restyleUpdate['marker.size'][k_idx] = (DEFAULT_MARKER_SIZE); // Reset size
                        restyleUpdate['marker.line.width'][k_idx] = (DEFAULT_MARKER_LINE_WIDTH);
                        restyleUpdate['marker.line.color'][k_idx] = (DEFAULT_MARKER_LINE_COLOR);
                        restyleUpdate['textfont.color'][k_idx] = ('rgba(255,255,255,0.1)');

                        animateDataPayload[k_idx] = {
                             x: [null], y: [null], text: [''],
                             marker: {
                                 color: '#333333', opacity: 0.1, size: DEFAULT_MARKER_SIZE,
                                 line: { width: DEFAULT_MARKER_LINE_WIDTH, color: DEFAULT_MARKER_LINE_COLOR }
                             },
                             textfont: { color: 'rgba(255,255,255,0.1)' }
                        };
                    }
                }
            }
        });


        if (traceIndicesToUpdateInPlotly.length === 0) {
            return window.dash_clientside.no_update;
        }

        const DURATION_THRESHOLD_MS = 600; let animationDuration = 0;
        if (window.dash_clientside.clientside._trackMapResizedRecently) {
            animationDuration = 0;
            if(window.dash_clientside.clientside._resizeTimeoutId) { clearTimeout(window.dash_clientside.clientside._resizeTimeoutId); }
            window.dash_clientside.clientside._resizeTimeoutId = setTimeout(() => { window.dash_clientside.clientside._trackMapResizedRecently = false; }, 500);
        } else if (updateIntervalDuration && updateIntervalDuration > DURATION_THRESHOLD_MS) { animationDuration = Math.max(50, updateIntervalDuration * 0.90);
        } else if (updateIntervalDuration) { animationDuration = Math.min(50, updateIntervalDuration * 0.5); if (animationDuration < 20) animationDuration = 0; }

        if (animationDuration > 0 && Plotly.animate) {
            try { Plotly.animate(gd, { data: animateDataPayload, traces: traceIndicesToUpdateInPlotly }, { transition: { duration: animationDuration, easing: 'linear' }, frame: { duration: animationDuration, redraw: false } }); } catch (e) { console.error('[JS animateCarMarkers] Error during Plotly.animate:', e); }
        } else if (Plotly.restyle) {
            try { Plotly.restyle(gd, restyleUpdate, traceIndicesToUpdateInPlotly); } catch (e) { console.error('[JS animateCarMarkers] Error during Plotly.restyle:', e); }
        }
        return window.dash_clientside.no_update;
    },

    setupTrackMapResizeListener: function(figure) {
        const graphDivId = 'track-map-graph';
        const graphDiv = document.getElementById(graphDivId);
        if (typeof Plotly === 'undefined' || !Plotly) { return window.dash_clientside.no_update; }
        if (graphDiv && !window.dash_clientside.clientside._trackMapResizeObserver) {
             try {
                 const resizeObserver = new ResizeObserver(entries => {
                     const currentGraphDiv = document.getElementById(graphDivId);
                     if (currentGraphDiv && currentGraphDiv.offsetParent !== null) {
                         Plotly.Plots.resize(currentGraphDiv);
                         window.dash_clientside.clientside._trackMapResizedRecently = true;
                         if(window.dash_clientside.clientside._resizeTimeoutId) { clearTimeout(window.dash_clientside.clientside._resizeTimeoutId); }
                         window.dash_clientside.clientside._resizeTimeoutId = setTimeout(() => { window.dash_clientside.clientside._trackMapResizedRecently = false; }, 500);
                     }
                 });
                 const wrapperDiv = graphDiv.parentElement;
                 if (wrapperDiv) {
                     resizeObserver.observe(wrapperDiv);
                     window.dash_clientside.clientside._trackMapResizeObserver = resizeObserver;
                 }
             } catch (e) { console.error("[JS setupTrackMapResizeListener] Error setting up ResizeObserver:", e); }
        }
        return window.dash_clientside.no_update;
    },

    setupClickToFocusListener: function(figure) { // figure input is just to ensure it runs when graph is ready
        const graphDivId = 'track-map-graph';
        const gd = document.getElementById(graphDivId);
        const clickDataHolderId = 'js-click-data-holder'; // ID of the hidden div

        if (gd && typeof Plotly !== 'undefined') {
            if (gd.hasOwnProperty('_hasF1ClickFocusListenerPolling')) {
                // console.log('[JS ClickToFocusPolling] Listener already attached.');
                return window.dash_clientside.no_update;
            }

            // console.log('[JS ClickToFocusPolling] Setting up plotly_click listener for:', graphDivId);
            gd.on('plotly_click', function(data) {
                if (data.points.length > 0) {
                    const point = data.points[0];
                    // Car marker traces have a 'uid' property (which should be the RacingNumber as string)
                    if (point.data && typeof point.data.uid === 'string' && point.data.uid.trim() !== "") {
                        const carNumberStr = point.data.uid; // This is the RacingNumber as a string
                        const timestamp = new Date().getTime();
                        // Ensure the JSON structure matches what Python expects: { carNumber: "..." }
                        const clickEventData = JSON.stringify({ carNumber: carNumberStr, ts: timestamp });

                        const dataHolderDiv = document.getElementById(clickDataHolderId);
                        if (dataHolderDiv) {
                            // console.log('[JS ClickToFocusPolling] Car clicked. Writing to js-click-data-holder:', clickEventData);
                            dataHolderDiv.textContent = clickEventData;
                        } else {
                            console.warn('[JS ClickToFocusPolling] js-click-data-holder div not found.');
                        }
                    }
                }
            });
            gd._hasF1ClickFocusListenerPolling = true; // Mark listener as attached
        } else {
            if (!gd) console.warn('[JS ClickToFocusPolling] Graph div not found for click listener.');
            if (typeof Plotly === 'undefined') console.warn('[JS ClickToFocusPolling] Plotly object not found for click listener.');
        }
        return window.dash_clientside.no_update;
    },

    pollClickDataAndUpdateStore: function(n_intervals, _ignored_clickDataFromDivChildren_via_state) {
        const clickDataHolderId = 'js-click-data-holder';
        const dataHolderDiv = document.getElementById(clickDataHolderId);
        let currentClickDataInDiv = null;

        if (dataHolderDiv) {
            currentClickDataInDiv = dataHolderDiv.textContent;
        }

        // console.log(`[JS PollAndUpdateStore ENTRY] Interval: ${n_intervals}, Data from Div: '${currentClickDataInDiv}'`);

        if (typeof window.dash_clientside.clientside._lastClickDataSentToStore === 'undefined') {
            window.dash_clientside.clientside._lastClickDataSentToStore = null;
            // console.log('[JS PollAndUpdateStore] Initialized _lastClickDataSentToStore to null.');
        }

        if (currentClickDataInDiv === null || typeof currentClickDataInDiv === 'undefined' || currentClickDataInDiv.trim() === "") {
            if (window.dash_clientside.clientside._lastClickDataSentToStore !== null && window.dash_clientside.clientside._lastClickDataSentToStore !== "") {
                // console.log('[JS PollAndUpdateStore] Div content is now empty/null. Clearing store.');
                window.dash_clientside.clientside._lastClickDataSentToStore = "";
                return "";
            }
            return window.dash_clientside.no_update;
        }

        if (currentClickDataInDiv !== window.dash_clientside.clientside._lastClickDataSentToStore) {
            // console.log('[JS PollAndUpdateStore] New/different click data found:', currentClickDataInDiv, "-> Updating store.");
            window.dash_clientside.clientside._lastClickDataSentToStore = currentClickDataInDiv;
            return currentClickDataInDiv;
        } else {
            return window.dash_clientside.no_update;
        }
    }
};