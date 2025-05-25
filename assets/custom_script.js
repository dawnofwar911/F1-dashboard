// assets/custom_script.js

if (!window.dash_clientside) { window.dash_clientside = {}; }

window.dash_clientside.clientside = {
    plotlyReactedMap: {}, // Stores the uirevision of the layout last used by Plotly.react
    plotlyReactedMapDataKey: {}, // Stores a key representing the data content last reacted to
    _trackMapResizedRecently: false,
    _resizeTimeoutId: null,
    _trackMapResizeObserver: null,

    animateCarMarkers: function (newCarData, trackMapVersion, existingFigureFromState, graphDivId, updateIntervalDuration) {
        const gd = document.getElementById(graphDivId);
        if (!gd) {
            console.warn("[JS animateCarMarkers] Graph div not found:", graphDivId);
            return window.dash_clientside.no_update;
        }

        let figureToProcess = existingFigureFromState; 
        
        if (!figureToProcess || !figureToProcess.layout || !figureToProcess.data) {
            console.warn("[JS animateCarMarkers] existingFigureFromState (figureToProcess) is null/undefined or lacks layout/data. This can happen on initial load if Python hasn't sent a figure yet. Waiting for a valid figure.");
            if (trackMapVersion && window.dash_clientside.clientside.plotlyReactedMap[graphDivId] && figureToProcess && figureToProcess.layout && figureToProcess.layout.uirevision && window.dash_clientside.clientside.plotlyReactedMap[graphDivId] !== figureToProcess.layout.uirevision) {
                 console.log("[JS animateCarMarkers] uirevision changed but figureToProcess is incomplete. Clearing reactedMap uirevision to force react on next valid figure.");
                 delete window.dash_clientside.clientside.plotlyReactedMap[graphDivId]; // Allow next react
                 delete window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId]; // Allow next react based on data
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

        // --- Full Redraw Logic (Plotly.react) ---
        if ( (trackMapVersion && currentLayoutUiRevisionFromPython && (lastReactedLayoutUiRevision !== currentLayoutUiRevisionFromPython)) || 
             (pythonFigureDataKey !== lastReactedDataKey) 
           ) {
            // Ensure figureToProcess.data is an array, even if empty, for Plotly.react
            const dataForReact = Array.isArray(figureToProcess.data) ? figureToProcess.data : [];
            
            // Log what data Plotly.react is about to use
            console.log(`[JS animateCarMarkers] Call Plotly.react. Reason: LayoutUirevChanged: ${lastReactedLayoutUiRevision !== currentLayoutUiRevisionFromPython}, DataKeyChanged: ${pythonFigureDataKey !== lastReactedDataKey}. PythonLayoutUirev: ${currentLayoutUiRevisionFromPython}, PythonDataKey: ${pythonFigureDataKey}`);
            console.log("[JS React Path] figureToProcess.data (from State) before Plotly.react:", 
                dataForReact.map(t => `${t.name || 'Unnamed'}(UID: ${t.uid || 'NoUID'}, Visible: ${t.visible === undefined ? 'N/A' : t.visible})`)
            );

            try {
                Plotly.react(gd, dataForReact, figureToProcess.layout, figureToProcess.config || {});
                window.dash_clientside.clientside.plotlyReactedMap[graphDivId] = currentLayoutUiRevisionFromPython;
                window.dash_clientside.clientside.plotlyReactedMapDataKey = window.dash_clientside.clientside.plotlyReactedMapDataKey || {};
                window.dash_clientside.clientside.plotlyReactedMapDataKey[graphDivId] = pythonFigureDataKey;
                reactedInThisCall = true;
                console.log(`[JS animateCarMarkers] Plotly.react successful.`);
            } catch (e) {
                console.error('[JS animateCarMarkers] Error during Plotly.react:', e, 'Figure (stringified):', JSON.stringify(figureToProcess));
                return window.dash_clientside.no_update; 
            }
        }
        // --- End Full Redraw Logic ---
        
        if (reactedInThisCall && (!newCarData || Object.keys(newCarData).length === 0 || (newCarData.status && newCarData.status !== 'active'))) {
            console.log("[JS animateCarMarkers] Just called Plotly.react, no immediate car data for animation. Returning.");
            return window.dash_clientside.no_update;
        }
        
        if (newCarData && newCarData.status === 'reset_map_display') {
            console.log("[JS animateCarMarkers] Received 'reset_map_display' signal.");
            return window.dash_clientside.no_update;
        }

        if (!newCarData || Object.keys(newCarData).length === 0 || (newCarData.status && newCarData.status !== 'active')) {
            return window.dash_clientside.no_update;
        }

        // --- Animation/Restyle Logic for Car Markers ---
        // Use figureToProcess.data to understand the intended structure and indices of car traces.
        // The actual Plotly.animate/restyle calls operate on 'gd'.
        const traces_in_python_figure = figureToProcess.data; 
        
        if (!traces_in_python_figure || !Array.isArray(traces_in_python_figure) || traces_in_python_figure.length === 0) {
            console.warn("[JS animateCarMarkers] Animation path: figureToProcess.data (from State) is empty. Cannot animate cars.");
            return window.dash_clientside.no_update;
        }
        
        // This log shows what Python sent (figureToProcess.data), which should include updated yellow sectors if any.
        // The *visuals* depend on whether 'gd' (the DOM Plotly object) correctly reflects this after Dash updates the prop.
        console.log("[JS Animate/Restyle Path] Figure from Python (figureToProcess.data) to be used for animation logic:", 
            traces_in_python_figure.map(t => `${t.name || 'Unnamed'}(UID: ${t.uid || 'NoUID'}, Visible: ${t.visible === undefined ? 'N/A' : t.visible})`)
        );

        let uidToTraceIndex = {}; 
        traces_in_python_figure.forEach((trace, index) => {
            if (trace && typeof trace.uid === 'string' && trace.uid.trim() !== "") { 
                uidToTraceIndex[trace.uid] = index;
            }
        });

        let traceIndicesToUpdateInPlotly = []; 
        let restyleUpdate = { x: [], y: [], text: [], 'marker.color': [], 'marker.opacity': [], 'textfont.color': [] };
        let animateDataPayload = []; 

        for (const carUID in newCarData) {
            if (carUID === 'status' || carUID === 'timestamp') continue;
            const carInfo = newCarData[carUID];
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

                restyleUpdate.x[k_idx] = (typeof carInfo.x === 'number' ? [carInfo.x] : [null]);
                restyleUpdate.y[k_idx] = (typeof carInfo.y === 'number' ? [carInfo.y] : [null]);
                restyleUpdate.text[k_idx] = ([tla]);
                restyleUpdate['marker.color'][k_idx] = (markerColor);
                restyleUpdate['marker.opacity'][k_idx] = (markerOpacity);
                restyleUpdate['textfont.color'][k_idx] = (textFontColor);
                
                animateDataPayload[k_idx] = { x: restyleUpdate.x[k_idx], y: restyleUpdate.y[k_idx], text: restyleUpdate.text[k_idx], marker: { color: markerColor, opacity: markerOpacity }, textfont: { color: textFontColor } };
            }
        }
        
        traces_in_python_figure.forEach((trace, originalTraceIndex) => {
            if (trace && typeof trace.uid === 'string' && trace.uid.trim() !== "") { 
                if (!newCarData[trace.uid]) { 
                    if (gd.data && originalTraceIndex < gd.data.length) {
                        if (!traceIndicesToUpdateInPlotly.includes(originalTraceIndex)) {
                            traceIndicesToUpdateInPlotly.push(originalTraceIndex);
                        }
                        let k_idx = traceIndicesToUpdateInPlotly.indexOf(originalTraceIndex);
                        Object.keys(restyleUpdate).forEach(key => { while(restyleUpdate[key].length <= k_idx) restyleUpdate[key].push(undefined); });
                        while(animateDataPayload.length <= k_idx) animateDataPayload.push({});

                        restyleUpdate.x[k_idx] = ([null]); restyleUpdate.y[k_idx] = ([null]); restyleUpdate.text[k_idx] = (['']);
                        restyleUpdate['marker.color'][k_idx] = ('#333333'); restyleUpdate['marker.opacity'][k_idx] = (0.1); restyleUpdate['textfont.color'][k_idx] = ('rgba(255,255,255,0.1)');
                        animateDataPayload[k_idx] = { x: [null], y: [null], text: [''], marker: { color: '#333333', opacity: 0.1 }, textfont: { color: 'rgba(255,255,255,0.1)' } };
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
    }
};