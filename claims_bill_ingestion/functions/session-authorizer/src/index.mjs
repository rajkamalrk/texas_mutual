import jwt from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';

// Replace with your Okta domain and audience
const oktaDomain = process.env.ISSUER_URL;
const audience = process.env.AUDIENCE;;

const client = jwksClient({
  jwksUri: `${oktaDomain}/v1/keys`
});

// console.log('client', client);
function getKey(header, callback) {
  // console.log('header', header);
  // console.log('callback', callback);

  client.getSigningKey(header.kid, (err, key) => {
    if (err) {
      callback(err);
    } else {
      const signingKey = key.getPublicKey();
      // console.log('signingKey', signingKey);
      callback(null, signingKey);
    }
  });
}

export const handler = async (event) => {
  try {
    const token = event.authorizationToken.split(' ')[1];
    // console.log('event', token);
    
    const decoded = await new Promise((resolve, reject) => {
      jwt.verify(token, getKey, {
        audience: audience,
        issuer: `${oktaDomain}`
      }, (err, decoded) => {
        if (err) {
          reject(err);
        } else {
          resolve(decoded);
        }
      });
    });
  
    // console.log('decoded', decoded);
    return await generatePolicy(decoded.sub, 'Allow', event.methodArn);
  } catch (err) {
    console.error('Authorization error:', err);
    throw new Error('Unauthorized');
  }
};

async function generatePolicy(principalId, effect, resource) {
  const authResponse = {
    principalId: principalId,
  };

  if (effect && resource) {
    const policyDocument = {
      Version: '2012-10-17',
      Statement: [{
        Action: 'execute-api:Invoke',
        Effect: effect,
        Resource: resource,
      }],
    };
    // console.log('policyDocument', policyDocument);
    authResponse.policyDocument = policyDocument;
  }
  // console.log('policyDocument authResponse', authResponse);
  return authResponse;
}
