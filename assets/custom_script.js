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

        // --- Prepare data updates (common for both methods) ---
        let traceIndicesToUpdate = [];
        // For restyle:
        let restyleUpdatePayload = { x: [], y: [], text: [], 'marker.color': [], 'marker.opacity': [] };
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

                // Data for restyle
                restyleUpdatePayload.x.push([car.x]);
                restyleUpdatePayload.y.push([car.y]);
                restyleUpdatePayload.text.push([car.tla]);
                restyleUpdatePayload['marker.color'].push(car.color);
                restyleUpdatePayload['marker.opacity'].push((car.status && (car.status === 'pit' || car.status === 'retired' || car.status === 'stopped' || car.status === 'out')) ? 0.3 : 1.0);

                // Data for animate (needs to be structured per trace)
                let singleAnimateTraceUpdate = {
                    x: [car.x], // Double bracket for frame data
                    y: [car.y],
                    text: [car.tla],
                    // To keep it simpler and focus on position:
                    marker: { // This will apply to the trace when animate is called for its data
                        color: car.color,
                        opacity: (car.status && (car.status === 'pit' || car.status === 'retired' || car.status === 'stopped' || car.status === 'out')) ? 0.3 : 1.0,
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

        if (updateIntervalDuration && updateIntervalDuration > DURATION_THRESHOLD_MS) {
            // Low frequency updates: Use Plotly.animate for smooth transitions
            const animationDuration = Math.max(50, updateIntervalDuration * 0.9); // e.g., 90% of interval
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
                Plotly.restyle(gd, restyleUpdatePayload, traceIndicesToUpdate);
            } catch (e) {
                console.error('Error during Plotly.restyle:', e);
            }
        }

        return window.dash_clientside.no_update;
    }
};