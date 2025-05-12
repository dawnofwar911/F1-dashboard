// In assets/custom_script.js

if (!window.dash_clientside) { window.dash_clientside = {}; }
window.dash_clientside.clientside = {
    plotlyReactedMap: {},

    animateCarMarkers: function (newCarData, existingFigure, graphDivId, updateIntervalDuration) {
        if (!newCarData || Object.keys(newCarData).length === 0) {
            return window.dash_clientside.no_update;
        }

        const gd = document.getElementById(graphDivId);
        if (!gd) {
            // console.warn("Graph div not found:", graphDivId);
            return window.dash_clientside.no_update;
        }

        // Ensure Plotly object is fully initialized (same as before)
        let needsReactCheck = !gd.data || !gd.layout;
        const currentUiRevision = existingFigure?.layout?.uirevision;
        const reactedUiRevision = window.dash_clientside.clientside.plotlyReactedMap[graphDivId];

        if (needsReactCheck && currentUiRevision && reactedUiRevision !== currentUiRevision) {
            if (existingFigure?.data && existingFigure?.layout) {
                try {
                    Plotly.react(gd, existingFigure.data, existingFigure.layout, existingFigure.config);
                    window.dash_clientside.clientside.plotlyReactedMap[graphDivId] = currentUiRevision;
                    needsReactCheck = !gd.data || !gd.layout;
                } catch (e) {
                    console.error('Error during Plotly.react:', e);
                    return window.dash_clientside.no_update;
                }
            } else {
                return window.dash_clientside.no_update;
            }
        } else if (needsReactCheck) {
            return window.dash_clientside.no_update;
        }

        const dataArray = gd.data || gd._fullData;
        if (!dataArray || !Array.isArray(dataArray) || dataArray.length === 0) {
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
                'modebar': false
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

        if (updateIntervalDuration && updateIntervalDuration > DURATION_THRESHOLD_MS) {
            // Low frequency updates: Use Plotly.animate for smooth transitions
            const animationDuration = Math.max(50, updateIntervalDuration * 0.95); // e.g., 90% of interval
            // console.log(`Using Plotly.animate, duration: ${animationDuration}ms`);

            try {
                Plotly.animate(gd, {
                    data: animateDataUpdates, // Array of update objects, one per trace
                    traces: traceIndicesToUpdate // Indices of traces to apply these updates to
                }, {
                    transition: { duration: animationDuration, easing: 'linear' },
                    frame: { duration: animationDuration, redraw: false }
                });
            } catch (e) {
                console.error('Error during Plotly.animate:', e);
            }
        } else {
            // High frequency updates: Use Plotly.restyle for directness
            // console.log(`Using Plotly.restyle, interval: ${updateIntervalDuration}ms`);
            try {
                 // Plotly.restyle expects update payload structured by attribute
                 // and the final argument is an array of trace indices to apply to.
                 // The restyleUpdatePayload is already structured by attribute,
                 // and each attribute's array of values should map to traceIndicesToUpdate.
                Plotly.restyle(gd, finalRestylePayload, traceIndicesToUpdate);
            } catch (e) {
                console.error('Error during Plotly.restyle:', e);
            }
        }

        return window.dash_clientside.no_update;
    }
};