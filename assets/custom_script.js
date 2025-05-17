// In assets/custom_script.js

if (!window.dash_clientside) { window.dash_clientside = {}; }

window.dash_clientside.clientside = {
    plotlyReactedMap: {},
    _trackMapResizedRecently: false,
    _resizeTimeoutId: null,
    _trackMapResizeObserver: null,

    animateCarMarkers: function (newCarData, trackMapVersion, existingFigure, graphDivId, updateIntervalDuration) {
        // trackMapVersion is new. Its change signifies initialize_track_map (Python) has outputted a new figure.
        // When this happens, existingFigure should be the very latest.

        // console.log(`[JS animateCarMarkers] Version: ${trackMapVersion}, Python uirevision: ${existingFigure?.layout?.uirevision}, JS Cache uirevision: ${window.dash_clientside.clientside.plotlyReactedMap[graphDivId]}`);
        // console.log("[JS animateCarMarkers] newCarData:", newCarData);

        const gd = document.getElementById(graphDivId);
        if (!gd) {
            // console.warn("[JS animateCarMarkers] Graph div not found:", graphDivId);
            return window.dash_clientside.no_update;
        }
         if (!existingFigure) { // If existingFigure is null from the start
            // console.warn("[JS animateCarMarkers] existingFigure is null. Cannot proceed.");
            return window.dash_clientside.no_update;
        }


        const currentUiRevision = existingFigure.layout?.uirevision; // Use optional chaining
        const reactedUiRevision = window.dash_clientside.clientside.plotlyReactedMap[graphDivId];

        // --- Full Redraw Logic (Plotly.react) ---
        if (currentUiRevision && reactedUiRevision !== currentUiRevision && existingFigure.layout && existingFigure.data) {
            console.log(`[JS animateCarMarkers] Uirevision changed. Python: ${currentUiRevision}, JS Cache: ${reactedUiRevision}. Calling Plotly.react.`);
            try {
                Plotly.react(gd, existingFigure.data, existingFigure.layout, existingFigure.config || {});
                window.dash_clientside.clientside.plotlyReactedMap[graphDivId] = currentUiRevision;
                console.log(`[JS animateCarMarkers] Plotly.react successful. Updated JS reactedUiRevision to: ${currentUiRevision}`);
            } catch (e) {
                console.error('[JS animateCarMarkers] Error during Plotly.react:', e, 'Figure:', JSON.parse(JSON.stringify(existingFigure)));
                return window.dash_clientside.no_update;
            }
        }
        // --- End Full Redraw Logic ---

        // --- Handle Reset Signal ---
        if (newCarData && newCarData.status === 'reset_map_display') {
            console.log("[JS animateCarMarkers] 'reset_map_display' signal. Map should be reset by uirevision change & Plotly.react. No further animation.");
            return window.dash_clientside.no_update; // Figure was already set by Python Output or Plotly.react
        }

        // --- Animation/Restyle Logic for Car Markers ---
        if (!newCarData || Object.keys(newCarData).length === 0 || (newCarData && newCarData.status)) {
            return window.dash_clientside.no_update;
        }

        const currentGraphTraces = gd.data || gd._fullData || existingFigure.data;
        if (!currentGraphTraces || !Array.isArray(currentGraphTraces) || currentGraphTraces.length === 0) {
            // console.warn("[JS animateCarMarkers] Graph has no data traces for animation.");
            return window.dash_clientside.no_update;
        }

        let uidToTraceIndex = {};
        let carTracesExist = false;
        currentGraphTraces.forEach((trace, index) => {
            if (index > 0 && trace && typeof trace.uid !== 'undefined') { // Assuming trace 0 is track
                uidToTraceIndex[trace.uid] = index;
                carTracesExist = true;
            }
        });

        if (!carTracesExist && Object.keys(newCarData).some(key => key !== 'status' && key !== 'timestamp')) {
            // console.warn("[JS animateCarMarkers] No car traces with UIDs in graph, but received car data.");
            return window.dash_clientside.no_update;
        }
        if (!carTracesExist) {
             return window.dash_clientside.no_update;
        }

        let traceIndicesForPlotly = [];
        let restyleData = { x: [], y: [], text: [], 'marker.color': [], 'marker.opacity': [], 'textfont.color': [] };
        let animateFrames = [];
        let carsProcessedCount = 0;

        for (const carUID in newCarData) {
            if (carUID === 'status' || carUID === 'timestamp') continue;
            const carInfo = newCarData[carUID];
            const traceIndex = uidToTraceIndex[carUID];

            if (traceIndex !== undefined) {
                carsProcessedCount++;
                traceIndicesForPlotly.push(traceIndex);
                const tla = (typeof carInfo.tla === 'string' && carInfo.tla.trim() !== '') ? carInfo.tla : carUID.toString();
                const markerColor = (typeof carInfo.color === 'string' && carInfo.color.startsWith('#')) ? carInfo.color : '#808080';
                const carStatus = (typeof carInfo.status === 'string') ? carInfo.status.toLowerCase() : "";
                const isDimmed = carStatus.includes('retired') || carStatus.includes('pit') || carStatus.includes('stopped');
                const markerOpacity = isDimmed ? 0.3 : 1.0;
                const textFontColor = `rgba(255, 255, 255, ${isDimmed ? 0.35 : 1.0})`;

                restyleData.x.push(typeof carInfo.x === 'number' ? [carInfo.x] : [null]);
                restyleData.y.push(typeof carInfo.y === 'number' ? [carInfo.y] : [null]);
                restyleData.text.push([tla]);
                restyleData['marker.color'].push(markerColor);
                restyleData['marker.opacity'].push(markerOpacity);
                restyleData['textfont.color'].push(textFontColor);

                animateFrames.push({
                    x: typeof carInfo.x === 'number' ? [carInfo.x] : [null],
                    y: typeof carInfo.y === 'number' ? [carInfo.y] : [null],
                    text: [tla],
                    marker: { color: markerColor, opacity: markerOpacity },
                    textfont: { color: textFontColor }
                });
            }
        }
        
        currentGraphTraces.forEach((trace, index) => {
            if (index > 0 && trace && typeof trace.uid !== 'undefined') {
                if (!newCarData[trace.uid]) { 
                    if (!traceIndicesForPlotly.includes(index)) {
                        carsProcessedCount++;
                        traceIndicesForPlotly.push(index);
                        restyleData.x.push([null]);
                        restyleData.y.push([null]);
                        restyleData.text.push(['']);
                        restyleData['marker.color'].push('#333333');
                        restyleData['marker.opacity'].push(0.1);
                        restyleData['textfont.color'].push('rgba(255,255,255,0.1)');
                        animateFrames.push({
                            x: [null], y: [null], text: [''],
                            marker: { color: '#333333', opacity: 0.1 },
                            textfont: { color: 'rgba(255,255,255,0.1)' }
                        });
                    }
                }
            }
        });

        if (carsProcessedCount === 0) {
            return window.dash_clientside.no_update;
        }

        const DURATION_THRESHOLD_MS = 600;
        let animationDuration = 0;
        if (window.dash_clientside.clientside._trackMapResizedRecently) {
            animationDuration = 0;
            if(window.dash_clientside.clientside._resizeTimeoutId) {
                clearTimeout(window.dash_clientside.clientside._resizeTimeoutId);
            }
            window.dash_clientside.clientside._resizeTimeoutId = setTimeout(() => {
                window.dash_clientside.clientside._trackMapResizedRecently = false;
            }, 100);
        } else if (updateIntervalDuration && updateIntervalDuration > DURATION_THRESHOLD_MS) {
            animationDuration = Math.max(50, updateIntervalDuration * 0.90);
        } else if (updateIntervalDuration) {
            animationDuration = Math.min(50, updateIntervalDuration * 0.5);
            if (animationDuration < 20) animationDuration = 0;
        }

        if (animationDuration > 0) {
            try {
                Plotly.animate(gd, { data: animateFrames, traces: traceIndicesForPlotly }, {
                    transition: { duration: animationDuration, easing: 'linear' },
                    frame: { duration: animationDuration, redraw: false }
                });
            } catch (e) { console.error('[JS animateCarMarkers] Error during Plotly.animate:', e); }
        } else {
            try {
                // Filter restyleData to only include data for traces actually being updated in this call
                const finalRestyleData = {};
                Object.keys(restyleData).forEach(key => {
                    finalRestyleData[key] = [];
                });

                for(let i=0; i < traceIndicesForPlotly.length; i++) {
                    Object.keys(restyleData).forEach(key => {
                        finalRestyleData[key].push(restyleData[key][i]);
                    });
                }
                Plotly.restyle(gd, finalRestyleData, traceIndicesForPlotly);
            } catch (e) { console.error('[JS animateCarMarkers] Error during Plotly.restyle:', e); }
        }
        return window.dash_clientside.no_update;
    },

    setupTrackMapResizeListener: function(figure) {
        const graphDivId = 'track-map-graph';
        const graphDiv = document.getElementById(graphDivId);

        if (typeof Plotly === 'undefined' || !Plotly) {
            // console.warn('[JS setupTrackMapResizeListener] Plotly object not found or not initialized.');
            return window.dash_clientside.no_update;
        }

        if (graphDiv && !window.dash_clientside.clientside._trackMapResizeObserver) {
            // console.log('[JS setupTrackMapResizeListener] Attaching ResizeObserver.');
            try {
                const resizeObserver = new ResizeObserver(entries => {
                    const currentGraphDiv = document.getElementById(graphDivId);
                    if (currentGraphDiv && currentGraphDiv.offsetParent !== null) { 
                        Plotly.Plots.resize(currentGraphDiv);
                        window.dash_clientside.clientside._trackMapResizedRecently = true;
                        if(window.dash_clientside.clientside._resizeTimeoutId) {
                            clearTimeout(window.dash_clientside.clientside._resizeTimeoutId);
                        }
                        window.dash_clientside.clientside._resizeTimeoutId = setTimeout(() => {
                            window.dash_clientside.clientside._trackMapResizedRecently = false;
                        }, 500);
                    }
                });
                const wrapperDiv = graphDiv.parentElement;
                if (wrapperDiv) {
                    resizeObserver.observe(wrapperDiv);
                    window.dash_clientside.clientside._trackMapResizeObserver = resizeObserver;
                } else {
                    // console.warn('[JS setupTrackMapResizeListener] Could not find wrapper div for ResizeObserver.');
                }
            } catch (e) {
                console.error("[JS setupTrackMapResizeListener] Error setting up ResizeObserver:", e);
            }
        }
        return window.dash_clientside.no_update;
    }
};