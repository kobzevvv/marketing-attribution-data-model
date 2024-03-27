window.dataLayer = window.dataLayer || [];
document.addEventListener('gform_confirmation_loaded', async function(event) {
    var formId = event.detail.formId;
    if (formId == '1') { // Replace '1' with your form ID
        var emailField = document.getElementById('your_email_field_id'); // Adjust the field ID
        if (emailField) {
            var hashedEmail = await hashEmail(emailField.value);
            window.dataLayer.push({
                'event': 'formSubmission',
                'formID': formId,
                'hashedEmail': hashedEmail
            });
        }
    }
})
