While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   This connector syncs:
      Users
      Groups
      Roles

2. Can the connector provision any resources? If so, which ones? 

   -Yes, this update includes account and entitlement provisioning
    What you can do is: Create a user, add a user to a group, add roles to a user, revoke user from a group,
    revoke user's roles, also you can't revoke roles that are heritated.

## Connector credentials 

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

  -ServiceNow deployment ID
  -Username and password.   

2. For each item in the list above: 

   * How does a user create or look up that credential or info? Please include links to (non-gated) documentation, screenshots (of the UI or of gated docs), or a video of the process. 

   -ServiceNow deployment ID: This is found in the URL of your ServiceNow instance (e.g., if your URL is https://example12345.service-now.com, your deployment ID is example12345).
   -Username and Password: You'll need the username and password for a user within your ServiceNow instance. This user must have either the Admin role or specific access control list (ACL) permissions.

   * Does the credential need any specific scopes or permissions? If so, list them here. 

    -The ServiceNow user whose credentials you use must have either the Admin role or be able to access the following ServiceNow tables:

    sys_user (Users)
    sys_user_role (Roles)
    sys_user_group (Groups)
    sys_user_grmember (Group membership)
    sys_user_has_role (User roles)
    sys_group_has_role (Group roles)

    -If you're configuring ServiceNow as an external ticketing provider, additional permissions are needed for the ServiceNow Table API and Service Catalog API.

    -Permissions for ServiceNow Table API (for external ticketing):

    The user needs read, write, and create permissions for the following tables:

    Choice (sys_choice) - Read
    Tag (label) - Create, read, write
    Label Entry (label_entry) - Create, read, write

    Note: The label_entry table has write ACLs for the table and table_key fields by default. If these cannot be changed, the ServiceNow request can still be made, but tagging won't be possible.

    -Permissions for ServiceNow Service Catalog API (for external ticketing):

    The user needs permissions for the following REST endpoints and methods:

    /api/sn_sc/servicecatalog/items - GET (Used to sync catalog items)
    /api/sn_sc/servicecatalog/items/<CATALOG ITEM ID> - GET (Used to fetch the configured catalog item)
    /api/sn_sc/servicecatalog/items/<CATALOG ITEM ID>/variables - GET (Used to get variables for a ServiceNow request)
    /api/sn_sc/servicecatalog/items/<CATALOG ITEM ID>/order_now - POST (Used to create the ServiceNow request)

    To configure these permissions:

    Access Analyzer for Table API Permissions:
        In the ServiceNow admin portal, navigate to All > Access Analyzer.
        Set Analyze by: User, Select user: (your integration user), Rule type: Table (record).
        Select the required tables (Choice, Tag, Label Entry) and click Analyze permissions.
        For any blocked access, click the permission's name to view Required ACL Roles and assign missing roles.

    Access Analyzer for Service Catalog API Permissions:
        In the ServiceNow admin portal, navigate to All > Access Analyzer.
        Set Analyze by: User, Select user: (your integration user), Rule type: REST endpoints.
        Enter each REST endpoint and REST endpoint method from the list above and click Analyze permissions.
        Confirm successful execution; if not, click the link under Operation to view Required ACL Roles and assign missing roles.

    Assigning User Roles:
        In the ServiceNow admin portal, navigate to All > System Security > Users and Groups > Users.
        Search for your user, click the User ID link, and then click Edit in the Roles section.
        Search for and add any missing roles, then click Save.

    Changing label_entry Table ACLs (if needed):
        In the ServiceNow admin portal, click your profile icon and Elevate Role, then select Security Admin and Update.
        Navigate to All > System Security > Access Control (ACL).
        Search for label_entry.table and click the write operation. Uncheck Active and click Update.
        Repeat for label_entry.table_key.

    Changing sys_choice.* Table ACLs (if needed):
        In the ServiceNow admin portal, click your profile icon and Elevate Role, then select Security Admin and Update.
        Navigate to All > System Security > Access Control (ACL).
        Search for sys_choice.read and click the read operation.
        In the Conditions section, add a role that is already assigned to your user.

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here. 

    Yes, the permissions are different.

    -Syncing (read) requires access to the sys_user, sys_user_role, sys_user_group, sys_user_grmember, sys_user_has_role, and sys_group_has_role tables.

    -Provisioning (read-write, specifically for external ticketing) requires additional create, read, and write permissions for the Choice, Tag, and Label Entry tables, as well as specific GET and POST permissions for the ServiceNow Service Catalog API endpoints.

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)  

   -The user providing the credentials for the connector setup needs either the Admin role in ServiceNow or specific access control list (ACL) permissions as detailed above. To configure the necessary ACLs and assign roles, a user with the Security Admin role might be required to elevate their role in ServiceNow.