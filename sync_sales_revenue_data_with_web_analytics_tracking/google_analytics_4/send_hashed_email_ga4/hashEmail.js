async function hashEmail(email) {
    const msgBuffer = new TextEncoder().encode(email); // Encode as UTF-8
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer); // Hash the email
    const hashArray = Array.from(new Uint8Array(hashBuffer)); // Convert to byte array
    const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join(''); // Convert to hexadecimal
    return hashHex;
}