// Function to retrieve the userId from Google Analytics 4
// It returns a promise that will resolve to the userId when available

function getGAUserId() {
    return new Promise((resolve) => {
      // Access the Google Analytics data for the current user
      ga(function(tracker) {
        var userId = tracker.get('userId');
        resolve(userId || '');
      });
    });
  }