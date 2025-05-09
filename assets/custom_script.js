// In assets/custom_script.js

if (!window.dash_clientside) { window.dash_clientside = {}; }
window.dash_clientside.clientside = {
    plotlyReactedMap: {},

    animateCarMarkers: function (newCarData, existingFigure, graphDivId) {
        if (!newCarData || Object.keys(newCarData).length === 0) { return window.dash_clientside.no_update; }

        const gd = document.getElementById(graphDivId);
        if (!gd) { /* console.warn... */ return window.dash_clientside.no_update; }

        let needsReactCheck = !gd.data || !gd.layout;
        const currentUiRevision = existingFigure?.layout?.uirevision;
        const reactedUiRevision = window.dash_clientside.clientside.plotlyReactedMap[graphDivId];

        if (needsReactCheck && currentUiRevision && reactedUiRevision !== currentUiRevision) {
            // console.warn(`Plotly properties missing... Attempting Plotly.react...`);
            if (existingFigure?.data && existingFigure?.layout) {
                try {
                    Plotly.react(gd, existingFigure.data, existingFigure.layout);
                    // console.log('After Plotly.react: .data=', gd.data, ' .layout=', gd.layout ? 'Exists' : 'Missing');
                    window.dash_clientside.clientside.plotlyReactedMap[graphDivId] = currentUiRevision;
                    needsReactCheck = !gd.data || !gd.layout;
                } catch (e) { /* console.error... */ return window.dash_clientside.no_update; }
            } else { /* console.warn... */ return window.dash_clientside.no_update; }
        } else if (needsReactCheck) { /* console.warn... */ return window.dash_clientside.no_update; }

        const dataArray = gd.data || gd._fullData;
        if (!dataArray || !Array.isArray(dataArray) || dataArray.length === 0) {
            // console.warn('Graph data still not ready after checks/react...');
            return window.dash_clientside.no_update;
        }

        // --- UID mapping ---
        let uidToTraceIndex = {};
        let foundUids = false;
        dataArray.forEach((trace, index) => {
            if (typeof trace === 'object' && trace !== null && trace.uid) {
                uidToTraceIndex[trace.uid] = index;
                foundUids = true;
            }
        });

        // --- ADD LOGGING HERE ---
        console.log(`UID Mapping Result: foundUids = ${foundUids}`);
        console.log('UID to Trace Index Map:', uidToTraceIndex); // Log the map itself
        // --- END LOGGING ---

        if (!foundUids) {
            console.warn('No traces with UIDs found. Cannot animate.');
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
        
        // --- Prepare updates ---
        let dataUpdates = [];
        let traceIndicesToUpdate = [];
        let logCount = 0; // Log only first few car coords
        console.log('Processing newCarData:', newCarData); // Log the incoming data

        for (const racingNumber in newCarData) {
            const car = newCarData[racingNumber];
            const traceIndex = uidToTraceIndex[racingNumber];

            if (traceIndex !== undefined) {
                // Check coordinate types (keep this check)
                // console.log(`Data for Car ${racingNumber}: X=${car.x} (type: ${typeof car.x}), Y=${car.y} (type: ${typeof car.y})`);
                if (typeof car.x !== 'number' || typeof car.y !== 'number' || isNaN(car.x) || isNaN(car.y)) {
                    console.error(`Invalid coordinates for Car ${racingNumber}! Skipping.`);
                    continue; // Skip this car if coords invalid
                }

                // --- MODIFIED traceUpdateData Structure ---
                let traceUpdateData = {
                    x: [car.x], // <<< CHANGE: Use single bracket array [value]
                    y: [car.y], // <<< CHANGE: Use single bracket array [value]
                    text: [car.tla], // <<< CHANGE: Use single bracket array [value]
                    // Marker object remains the same
                    marker: {
                        color: car.color,
                        opacity: (car.status && (car.status === 'pit' || car.status === 'retired' || car.status === 'stopped' || car.status === 'out')) ? 0.3 : 1.0,
                        size: 10, line: { width: 1, color: 'Black' }
                    }
                };
                // --- END MODIFICATION ---

                dataUpdates.push(traceUpdateData);
                traceIndicesToUpdate.push(traceIndex);

                // Optional logging for first few updates
                if (logCount < 3) {
                   console.log(` ==> Preparing update for Car ${racingNumber} (Index ${traceIndex}): X=${car.x}, Y=${car.y}`);
                   logCount++;
                }
            } else {
                // console.warn(`   => No trace index found for car ${racingNumber}.`);
            }
        }

        // --- Call Plotly.animate (remains the same) ---
        if (traceIndicesToUpdate.length > 0) {
            // console.log(`Calling Plotly.animate for ${traceIndicesToUpdate.length} traces.`);
            try {
                Plotly.animate(graphDivId, {
                    data: dataUpdates,
                    traces: traceIndicesToUpdate
                }, {
                    transition: { duration: 1200, easing: 'linear' },
                    frame: { duration: 1200, redraw: false }
                });
                // console.log('Plotly.animate call appears successful.');
            } catch (e) {
                console.error('Error during Plotly.animate:', e);
            }
        } else {
            // console.log("No matching traces found to update.");
        }

        return window.dash_clientside.no_update;
    }
};