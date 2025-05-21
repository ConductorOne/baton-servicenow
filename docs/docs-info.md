While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   This connector syncs:
      Users
      Groups
      Roles

2. Can the connector provision any resources? If so, which ones? 

   -Yes, this update includes account provisioning.
   Note: The new User will have Basic permissions
   -Also, this update includes revoke site admin permission from existing users
   Note: You can't revoke permission from a non-human identity or from yourself

## Connector credentials 

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

   -This connector requires an API Key. Args: --username
   -If you need provisioning, you'll need the email address of a user with Site Admin role. Args: --on_behalf_of_email

2. For each item in the list above: 

   * How does a user create or look up that credential or info? Please include links to (non-gated) documentation, screenshots (of the UI or of gated docs), or a video of the process. 

   -Log in to your Greenhouse account with an admin role.

   -In the top-right corner, click the Configure (⚙️) icon.

   -In the left-hand menu, go to Dev Center > API Credential Management.

   -Click the “Create New API Key” button.

   -Fill out the form:

    Type: Select Harvest.

    Partner: You can choose a partner

    Description: Add any extra context.

-Click “Manage Permissions”.

   Copy and securely store the API key — Greenhouse will not show it again.

   Check all required permissions. For simplicity, you can select “Select All”.

   Click Save.

   * Does the credential need any specific scopes or permissions? If so, list them here. 

      -In order to create an API Key, you'll have to be a site admin
      -In order to do provisioning, hte email have to be a site admin user

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here. 

   -No, both syncing and provisioning rely on the same Harvest API Key with appropriate permissions.

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)  

   -Must be a Site Admin in the Greenhouse organization.