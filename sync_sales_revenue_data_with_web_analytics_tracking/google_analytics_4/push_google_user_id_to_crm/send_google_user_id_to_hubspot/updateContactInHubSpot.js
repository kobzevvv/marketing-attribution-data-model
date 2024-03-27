// Function to send the userId to HubSpot CRM

// Function to send the userId to HubSpot CRM
function updateContactInHubSpot(userId) {
    const hubSpotEndpoint = 'https://api.hubapi.com/contacts/v1/contact/createOrUpdate/email/contact@example.com/';
    const hubSpotApiKey = 'your_hubspot_api_key';
  
    // Prepare the data payload with the userId for the HubSpot API
    const data = {
      properties: [
        {
          property: 'ga_user_id',
          value: userId
        }
      ]
    };
  
    // POST request to HubSpot's Contacts API with the userId
    fetch(`${hubSpotEndpoint}?hapikey=${hubSpotApiKey}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data)
    })
    .then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new Error('Network response was not ok.');
    })
    .then(responseData => {
      console.log('GA User ID updated in HubSpot:', responseData);
    })
    .catch(error => {
      console.error('Error updating contact in HubSpot:', error);
    });
  }