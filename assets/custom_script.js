// In assets/custom_script.js

if (!window.dash_clientside) { window.dash_clientside = {}; }

window.dash_clientside.clientside = {
    plotlyReactedMap: {},
    // Flag to indicate a resize just happened
    _trackMapResizedRecently: false, 
    _resizeTimeoutId: null,

    animateCarMarkers: function (newCarData, existingFigure, graphDivId, updateIntervalDuration) {
        console.log("[JS animateCarMarkers] Triggered. Python uirevision:", existingFigure?.layout?.uirevision, 
                    "JS Cache uirevision:", window.dash_clientside.clientside.plotlyReactedMap[graphDivId]);
        console.log("[JS animateCarMarkers] newCarData:", newCarData); // Log incoming newCarData

        const gd = document.getElementById(graphDivId);
        if (!gd) {
            console.warn("[JS animateCarMarkers] Graph div not found:", graphDivId);
            return window.dash_clientside.no_update;
        }

        const currentUiRevision = existingFigure?.layout?.uirevision;
        const reactedUiRevision = window.dash_clientside.clientside.plotlyReactedMap[graphDivId];

        // Condition to call Plotly.react:
        // - currentUiRevision must exist.
        // - currentUiRevision must be different from what JS last fully rendered.
        // - existingFigure.layout must exist (as Plotly.react needs it).
        if (currentUiRevision && reactedUiRevision !== currentUiRevision && existingFigure?.layout) {
            console.log("[JS animateCarMarkers] Uirevision changed OR first load with uirevision. Python:", currentUiRevision, "JS Cache:", reactedUiRevision);
            console.log("[JS animateCarMarkers] Attempting Plotly.react. existingFigure.data:", existingFigure.data); // Log what data is
            
            try {
                // Use existingFigure.data directly. For an empty map from Python (go.Figure with no traces), 
                // existingFigure.data should serialize to an empty array [].
                Plotly.react(gd, existingFigure.data, existingFigure.layout, existingFigure.config); 
                
                window.dash_clientside.clientside.plotlyReactedMap[graphDivId] = currentUiRevision;
                console.log("[JS animateCarMarkers] Plotly.react call successful. Updated JS reactedUiRevision cache to:", currentUiRevision);

                // For debugging: Log structure after react
                const postReactGd = document.getElementById(graphDivId);
                console.log("[JS animateCarMarkers] gd.data after Plotly.react (trace count):", postReactGd.data ? postReactGd.data.length : 'undefined');
                if (postReactGd.data && postReactGd.data.length === 0) {
                     console.log("[JS animateCarMarkers] Map appears empty after react (0 data traces). Annotation should be visible.");
                }

            } catch (e) {
                console.error('[JS animateCarMarkers] Error during Plotly.react:', e, 'Figure data:', existingFigure.data, 'Figure layout:', existingFigure.layout);
                return window.dash_clientside.no_update; 
            }
        } else {
            if (!currentUiRevision) console.log("[JS animateCarMarkers] Plotly.react NOT called: currentUiRevision is missing.");
            else if (reactedUiRevision === currentUiRevision) console.log("[JS animateCarMarkers] Plotly.react NOT called: uirevision from Python matches JS cache.");
            else if (!existingFigure?.layout) console.log("[JS animateCarMarkers] Plotly.react NOT called: existingFigure.layout is missing.");
        }
        
        if (newCarData && newCarData.status === 'reset_map_display') {
            console.log("[JS animateCarMarkers] Received 'reset_map_display' signal. Map should have been reset by Plotly.react if uirevision changed. Skipping car animation.");
            return window.dash_clientside.no_update; // Don't try to animate with this signal data
        }

        // --- Animation/Restyle Logic ---
        // This part should only run if the map has traces for cars
        
        if (!newCarData || Object.keys(newCarData).length === 0) {
            // console.log("[JS animateCarMarkers] No newCarData to animate.");
            return window.dash_clientside.no_update;
        }
        
        const currentGraphData = gd.data;
        if (!currentGraphData && Object.keys(newCarData).length > 0 ) { // if gd.data is null/undefined but we have car data, something is wrong after react
             console.warn("[JS animateCarMarkers] gd.data is null/undefined after react block, but received newCarData. Aborting animation for this cycle.");
             return window.dash_clientside.no_update;
        };
        
        const dataArray = gd.data || gd._fullData; // Use DOM's current data
        if (!dataArray || !Array.isArray(dataArray) || dataArray.length === 0) {
            console.warn("[JS animateCarMarkers] dataArray from gd.data is empty or invalid before building uidToTraceIndex.");
            return window.dash_clientside.no_update;
        }

        let uidToTraceIndex = {};
        let foundUids = false;
        dataArray.forEach((trace, index) => {
            if (typeof trace === 'object' && trace !== null && trace.uid) {
                uidToTraceIndex[trace.uid] = index;
                foundUids = true;
            }
        });

        if (!foundUids) {
            console.warn('No traces with UIDs found.');
            return window.dash_clientside.no_update;
        }
        
        if (gd.layout && (gd.layout.dragmode !== false || gd.layout.xaxis.fixedrange !== true)) {
            console.log("Forcing layout changes to disable zoom/pan via JS");
            Plotly.relayout(graphDivId, {
                'dragmode': false,
                'xaxis.fixedrange': true, // Disables zoom/pan on x-axis
                'yaxis.fixedrange': true,  // Disables zoom/pan on y-axis
                'modebar': false, 'autosizable': true, 'responsive': true
            });
        }

        // --- Prepare data updates (common for both methods) ---
        let traceIndicesToUpdate = [];
        let restyle_x = [];
        let restyle_y = [];
        let restyle_text = [];
        let restyle_marker_color = [];
        let restyle_marker_opacity = [];
        let restyle_textfont_color = []; // <<< To hold text colors for restyle

        // For animate:
        let animateDataUpdates = [];


        for (const racingNumber in newCarData) {
            const car = newCarData[racingNumber];
            const traceIndex = uidToTraceIndex[racingNumber];

            if (traceIndex !== undefined) {
                if (typeof car.x !== 'number' || typeof car.y !== 'number' || isNaN(car.x) || isNaN(car.y)) {
                    console.error(`Invalid coordinates for Car ${racingNumber}! Skipping.`);
                    continue;
                }
                
                const tla = (typeof car.tla === 'string' && car.tla.trim() !== '') ? car.tla : racingNumber.toString();
                const markerColor = (typeof car.color === 'string' && car.color.startsWith('#') && (car.color.length === 7 || car.color.length === 4)) ? car.color : '#808080';
                
                const car_status_string = (typeof car.status === 'string') ? car.status.toLowerCase() : ""; // Ensure lowercase and defined
                
                const isRetired = car_status_string.includes('retired');
                const isInPit = car_status_string.includes('pit');
                const isStopped = car_status_string.includes('stopped');
                
                const isDimmed = isRetired || isInPit || isStopped;
                const markerOpacityValue = isDimmed ? 0.3 : 1.0;

                const textBaseRgb = "255, 255, 255"; // Assuming base text is white (R, G, B)
                const textAlpha = isDimmed ? 0.35 : 1.0;    // Text alpha (opacity)
                const textFontColorValue = `rgba(${textBaseRgb}, ${textAlpha})`;


                // Data for restyle
                restyle_x.push([car.x]);
                restyle_y.push([car.y]);
                restyle_text.push([tla]);
                restyle_marker_color.push(markerColor);
                restyle_marker_opacity.push(markerOpacityValue);
                restyle_textfont_color.push(textFontColorValue); // <<< ADDED

                // Data for animate (needs to be structured per trace)
                let singleAnimateTraceUpdate = {
                    x: [car.x], // Your correction: single array for direct update
                    y: [car.y],
                    text: [tla],
                    marker: { 
                        color: markerColor,
                        opacity: markerOpacityValue,
                    },
                    textfont: { // <<< ADDED
                        color: textFontColorValue
                    }
                };
                animateDataUpdates.push(singleAnimateTraceUpdate);

                traceIndicesToUpdate.push(traceIndex);
            }
        }

        if (traceIndicesToUpdate.length === 0) {
            return window.dash_clientside.no_update;
        }

        // --- Conditional animation logic ---
        const DURATION_THRESHOLD_MS = 600; // If update interval is longer than this, use Plotly.animate
        const finalRestylePayload = {
            x: restyle_x,
            y: restyle_y,
            text: restyle_text,
            'marker.color': restyle_marker_color,
            'marker.opacity': restyle_marker_opacity,
            'textfont.color': restyle_textfont_color // <<< ADDED
        };
        
        let animationDuration = 0; 
        if (window.dash_clientside.clientside._trackMapResizedRecently) {
            animationDuration = 0;
            // console.log("animateCarMarkers: Snapping due to recent resize.");
            // Reset the flag after using it for one update cycle
            // Use a timeout to ensure it's reset after this update might have processed
            if(window.dash_clientside.clientside._resizeTimeoutId) {
                clearTimeout(window.dash_clientside.clientside._resizeTimeoutId);
            }
            window.dash_clientside.clientside._resizeTimeoutId = setTimeout(() => {
                window.dash_clientside.clientside._trackMapResizedRecently = false;
            }, 100); // Reset after a short delay
        } else if (updateIntervalDuration && updateIntervalDuration > DURATION_THRESHOLD_MS) {
            animationDuration = Math.max(50, updateIntervalDuration * 0.90); 
        } else if (updateIntervalDuration) { 
            animationDuration = Math.min(50, updateIntervalDuration * 0.5); 
        }
        
        const dataArrayForAnimation = gd.data || gd._fullData;
        console.log("JS: gd.data before animation/restyle (trace count):", dataArrayForAnimation ? dataArrayForAnimation.length : 'undefined');
        if (dataArrayForAnimation && dataArrayForAnimation.length > 0) {
    console.log("JS: First trace name before animation/restyle:", dataArrayForAnimation[0]?.name); // e.g., "Track" or car TLA
        }

        if (animationDuration > 0) {
            try {
                Plotly.animate(gd, 
                    { data: animateDataUpdates, traces: traceIndicesToUpdate }, 
                    {
                        transition: { duration: animationDuration, easing: 'linear' },
                        frame: { duration: animationDuration, redraw: false }
                    }
                );
            } catch (e) { console.error('Error during Plotly.animate:', e); }
        } else { 
            try {
                Plotly.restyle(gd, finalRestylePayload, traceIndicesToUpdate);
            } catch (e) { console.error('Error during Plotly.restyle:', e); }
        }
        return window.dash_clientside.no_update;
    },
    
    setupTrackMapResizeListener: function(figure) { 
        const graphDivId = 'track-map-graph'; 
        const graphDiv = document.getElementById(graphDivId);

        if (typeof Plotly === 'undefined') {
            console.warn('Plotly object not found for resize listener.');
            return dash_clientside.no_update;
        }

        if (graphDiv && !window.dash_clientside.clientside._trackMapResizeObserver) { 
            console.log('Attaching ResizeObserver to track map container\'s parent.');
            try {
                const resizeObserver = new ResizeObserver(entries => {
                    const currentGraphDiv = document.getElementById(graphDivId);
                    if (currentGraphDiv && currentGraphDiv.offsetParent !== null) { 
                        Plotly.Plots.resize(currentGraphDiv); 
                        // console.log("ResizeObserver: Called Plotly.Plots.resize.");
                        
                        // --- Option C from previous response for autoranging ---
                        Plotly.relayout(currentGraphDiv, {
                             'xaxis.autorange': true,
                             'yaxis.autorange': true
                        });
                        // console.log("ResizeObserver: Forced autorange after resize.");

                        // Set a flag that a resize occurred, for animateCarMarkers
                        window.dash_clientside.clientside._trackMapResizedRecently = true;
                        // console.log("ResizeObserver: _trackMapResizedRecently = true");

                        // Clear any pending timeout to reset the flag (safety)
                        if(window.dash_clientside.clientside._resizeTimeoutId) {
                            clearTimeout(window.dash_clientside.clientside._resizeTimeoutId);
                        }
                        // Automatically reset the flag after a short period,
                        // in case animateCarMarkers doesn't run immediately or misses it.
                        window.dash_clientside.clientside._resizeTimeoutId = setTimeout(() => {
                            window.dash_clientside.clientside._trackMapResizedRecently = false;
                            // console.log("ResizeObserver: Timeout reset _trackMapResizedRecently = false");
                        }, 500); // Reset after 500ms
                    }
                });
                
                const wrapperDiv = graphDiv.parentElement; 
                if (wrapperDiv) {
                    resizeObserver.observe(wrapperDiv); 
                    window.dash_clientside.clientside._trackMapResizeObserver = resizeObserver;
                } else {
                    console.warn('Could not find wrapper div for track map for ResizeObserver.');
                }
            } catch (e) {
                console.error("Error setting up ResizeObserver for track map:", e);
            }
        }
        return dash_clientside.no_update;
    }
};