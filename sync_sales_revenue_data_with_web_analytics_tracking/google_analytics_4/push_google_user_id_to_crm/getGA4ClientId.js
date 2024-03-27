// This function retrieves the GA4 (Google Analytics 4) client ID
// It returns a promise that will resolve to the client ID when available

function getGA4ClientId() {
    return new Promise((resolve) => {
        if (ga && ga.getAll) {
            let trackers = ga.getAll();
            if (trackers.length) {
                resolve(trackers[0].get('clientId'));
            }
        }
        resolve('');
    });
}
