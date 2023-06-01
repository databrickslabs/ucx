import concurrent.futures
import json
import math
import time
from functools import reduce
from os import getgrouplist
from typing import List

import requests
from pyspark.sql import DataFrame, session
from pyspark.sql.functions import array_contains, col, collect_set, column, lit
from pyspark.sql.types import MapType, StringType, StructField, StructType


class GroupMigration:
    def __init__(
        self,
        groupL: List[str],
        cloud: str,
        inventoryTableName: str,
        workspace_url: str,
        pat: str,
        spark: session.SparkSession,
        userName: str,
        checkTableACL: bool = False,
        numThreads: int = 32,
        autoGenerateList: bool = False,
        verbose: bool = False,
    ):
        self.groupL = groupL
        self.cloud = cloud
        self.workspace_url = workspace_url.rstrip("/")
        self.inventoryTableName = inventoryTableName
        self.token = pat
        self.headers = {"Authorization": "Bearer %s" % self.token}
        self.groupIdDict = {}  # map: group id => group name
        self.groupNameDict = {}  # map: group name => group id
        self.accountGroups_lower = {}
        self.groupMembers = {}  # map: group id => list[tuple[member name, memberid]]
        self.groupEntitlements = {}
        self.groupGroupList = []
        self.groupUserList = []
        self.groupSPList = []
        self.groupWSGIdDict = {}  # map : temp group id => temp group name
        self.groupWSGNameDict = {}  # map : temp group name => temp group id
        self.groupRoles = {}
        self.passwordPerm = {}
        self.clusterPerm = {}
        self.clusterPolicyPerm = {}
        self.warehousePerm = {}
        self.dashboardPerm = {}
        self.queryPerm = {}
        self.alertPerm = {}
        self.instancePoolPerm = {}
        self.jobPerm = {}
        self.expPerm = {}
        self.modelPerm = {}
        self.dltPerm = {}
        self.folderPerm = {}
        self.notebookPerm = {}
        self.filePerm = {}
        self.repoPerm = {}
        self.tokenPerm = {}
        self.secretScopePerm = {}
        self.dataObjectsPerm = []
        self.folderList = {}
        self.notebookList = {}
        self.fileList = {}
        self.spark = spark
        self.userName = userName
        self.checkTableACL = checkTableACL
        self.verbose = verbose
        self.numThreads = numThreads

        self.lastInventoryRun = None
        self.checkAllDB = False
        print(f"Clearing inventory table {self.inventoryTableName}")
        spark.sql(f"drop table if exists {self.inventoryTableName}")
        spark.sql(f"drop table if exists {self.inventoryTableName+'TableACL'}")

        # Check if we should automatically generate list, and do it immediately.
        # Implementers Note: Could change this section to a lazy calculation by setting groupL to nil or some sentinel value and adding checks before use.
        res = requests.get(
            f"{self.workspace_url}/api/2.0/preview/scim/v2/Me", headers=self.headers
        )
        # print(res.text)
        if res.status_code == 403:
            print("token not valid.")
            return
        if autoGenerateList:
            print(
                "autoGenerateList parameter is set to TRUE. Ignoring groupL parameter and instead will automatically generate list of migraiton groups."
            )
            self.groupL = self.findMigrationEligibleGroups()

        # Finish setting some params that depend on groupL
        if len(self.groupL) == 0:
            raise Exception("Migration group list (groupL) is empty!")

        self.TempGroupNames = ["db-temp-" + g for g in self.groupL]
        self.WorkspaceGroupNames = self.groupL

        print(
            f"Successfully initialized GroupMigration class with {len(self.groupL)} workspace-local groups to migrate. Groups to migrate:"
        )
        for i, group in enumerate(self.groupL, start=1):
            print(f"{i}. {group}")
        print(f"Done listing {len(self.groupL)} groups to migrate.")

    def findMigrationEligibleGroups(self):
        print("Begin automatic generation of all migration eligible groups.")
        # Get all workspace-local groups
        try:
            print("Executing request to list workspace groups")
            res = requests.get(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups",
                headers=self.headers,
            )
            if res.status_code != 200:
                raise Exception(
                    f"Bad status code. Expected: 200. Got: {res.status_code}"
                )

            resJson = res.json()

            allWsLocalGroups = [
                o["displayName"]
                for o in resJson["Resources"]
                if o["meta"]["resourceType"] == "WorkspaceGroup"
            ]

            # Prune special groups.
            prune_groups = ["admins", "users"]
            allWsLocalGroups = [g for g in allWsLocalGroups if g not in prune_groups]
            allWsLocalGroups_lower = [x.casefold() for x in allWsLocalGroups]
            allWsLocalGroups.sort()
            print(
                f"\nFound {len(allWsLocalGroups)} workspace local groups. Listing (alphabetical order): \n"
                + "\n".join(f"{i+1}. {name}" for i, name in enumerate(allWsLocalGroups))
            )

        except Exception as e:
            print(f"ERROR in retrieving workspace group list: {e}")
            raise

        # Now match against account groups.
        try:
            print("\nExecuting request to list account groups")
            res = requests.get(
                f"{self.workspace_url}/api/2.0/account/scim/v2/Groups",
                headers=self.headers,
            )
            if res.status_code != 200:
                raise Exception(
                    f"Bad status code. Expected: 200. Got: {res.status_code}"
                )
            resJson2 = res.json()
            allAccountGroups_lower = [
                r["displayName"].casefold() for r in resJson2["Resources"]
            ]
            allAccountGroups_lower.sort()

            # Get set intersection of both lists
            migration_eligible_lower = list(
                set(allWsLocalGroups_lower) & set(allAccountGroups_lower)
            )
            migration_eligible = [
                wsl
                for wsl in allWsLocalGroups
                if wsl.casefold() in migration_eligible_lower
            ]
            migration_eligible.sort()

            # Get list of items in allWsLocalGroups that are not in allAccountGroups
            not_in_account_groups = [
                group
                for group in allWsLocalGroups
                if group.casefold() not in allAccountGroups_lower
            ]
            not_in_account_groups.sort()

            # Print count and membership of not_in_account_groups
            print(
                f"Unable to match {len(not_in_account_groups)} current workspace-local groups. No matching account level group with the same name found. These groups WILL NOT MIGRATE:"
            )
            for i, group in enumerate(not_in_account_groups, start=1):
                print(f"{i}. {group} (WON'T MIGRATE)")

            if len(migration_eligible) > 0:
                # Print count and membership of intersection
                print(
                    f"\nFound {len(migration_eligible)} current workspace-local groups to account level groups. These groups WILL BE MIGRATED."
                )
                for i, group in enumerate(migration_eligible, start=1):
                    print(f"{i}. {group} (WILL MIGRATE)")
                print("")

                return migration_eligible
            else:
                print(
                    "There are no migration eligible groups. All existing workspace-local groups do not exist at the account level.\nNO MIGRATION WILL BE PERFORMED."
                )
                return []
        except Exception as e:
            print(f"ERROR in retrieving account group list : {e}")
            raise

    def validateWSGroup(self) -> list:
        try:
            res = requests.get(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups",
                headers=self.headers,
            )
            resJson = res.json()
            for e in resJson["Resources"]:
                if (
                    e["meta"]["resourceType"] == "Group"
                    and e["displayName"] in self.groupL
                ):
                    print(
                        f"{e['displayName']} is a Account level group, please provide workspace group"
                    )
                    return 0
            return 1
        except Exception as e:
            print(f"error in retrieving group objects : {e}")

    def getGroupObjects(self, groupFilterKeeplist) -> list:
        try:
            groupIdDict = {}
            groupMembers = {}
            groupEntitlements = {}
            groupRoles = {}
            res = requests.get(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups?attributes=id",
                headers=self.headers,
            )
            resJson = res.json()
            totalGroups = resJson["totalResults"]
            pages = totalGroups // 100
            # normalize case
            groupFilterKeeplist = [x.casefold() for x in groupFilterKeeplist]
            print(
                f"Total groups: {totalGroups}. Retrieving group details in chunks of 100"
            )
            for i in range(0, pages + 1):
                print(f"Retrieving the next 100 items from {str(i*100+1)}")

                res = requests.get(
                    f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups?startIndex={str(i*100+1)}&count=100",
                    headers=self.headers,
                )
                resJson = res.json()
                # Iterate over workspace groups, extracting useful info to vars above
                for e in resJson["Resources"]:
                    if not e["displayName"].casefold() in groupFilterKeeplist:
                        continue

                    groupIdDict[e["id"]] = e["displayName"]

                    # Get Group Members
                    members = []
                    try:
                        for mem in e["members"]:
                            members.append(
                                list([mem["display"], mem["value"], mem["$ref"]])
                            )
                    except KeyError:
                        pass
                    groupMembers[e["id"]] = members

                    # Get entitlements
                    entms = []
                    try:
                        for ent in e["entitlements"]:
                            entms.append(ent["value"])
                    except:
                        pass

                    groupEntitlements[e["id"]] = entms

                    # Get Roles (AWS only)
                    if self.cloud == "AWS":
                        entms = []
                        try:
                            for ent in e["roles"]:
                                entms.append(ent["value"])
                        except:
                            continue
                        if len(entms) == 0:
                            continue
                        groupRoles[e["id"]] = entms

                # Finally assign to self (Now that exception hasn't been thrown)
            self.groupIdDict = groupIdDict
            self.groupMembers = groupMembers
            self.groupEntitlements = groupEntitlements
            self.groupRoles = groupRoles
            # Create reverse of groupIdDict
            self.groupNameDict = {}
            for k, v in self.groupIdDict.items():
                self.groupNameDict[v] = k

        except Exception as e:
            print(f"error in retrieving group objects : {e}")

    # get list of users and service principals recursively for groups and nested groups
    def getRecursiveGroupMember(self, groupM: dict):
        groupPrincipalList = []
        for key, value in groupM.items():
            try:
                res = requests.get(
                    f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{key}",
                    headers=self.headers,
                )
                resJson = res.json()
                groupPrincipalList.append(resJson["displayName"])
            except Exception as e:
                print(f"error in retrieving group Names : {e}")
        self.groupGroupList.extend(groupPrincipalList)
        for key, value in groupM.items():
            userList = [u[1] for u in value if u[2].startswith("User")]
            spList = [u[1] for u in value if u[2].startswith("ServicePrincipal")]

            groupList = [u[1] for u in value if u[2].startswith("Group")]
            userPrincipalList = []
            spPrincipalList = []
            try:
                groupMembers = {}
                for g in groupList:
                    res = requests.get(
                        f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{g}",
                        headers=self.headers,
                    )
                    resJson = res.json()
                    members = []
                    try:
                        for mem in resJson["members"]:
                            members.append(
                                list([mem["display"], mem["value"], mem["$ref"]])
                            )
                    except KeyError:
                        pass
                    groupMembers[resJson["id"]] = members
            except Exception as e:
                print(f"error in retrieving nested group members : {e}")
            if len(groupMembers) > 0:
                self.getRecursiveGroupMember(groupMembers)

            for userid in userList:
                try:
                    res = requests.get(
                        f"{self.workspace_url}/api/2.0/preview/scim/v2/Users/{userid}",
                        headers=self.headers,
                    )
                    resJson = res.json()
                    userPrincipalList.append(resJson["userName"])
                except Exception as e:
                    print(f"error in retrieving user details : {e}")

            for spid in spList:
                try:
                    res = requests.get(
                        f"{self.workspace_url}/api/2.0/preview/scim/v2/ServicePrincipals/{spid}",
                        headers=self.headers,
                    )
                    resJson = res.json()
                    spPrincipalList.append(resJson["applicationId"])
                except Exception as e:
                    print(f"error in retrieving SP details : {e}")
            self.groupUserList.extend(userPrincipalList)
            self.groupSPList.extend(spPrincipalList)

    # getACL[n] family of functions extract the ACL from the converted json response into a standard format, filtering by groupL
    def getACL(self, acls: dict) -> list:
        aclList = []
        for acl in acls:
            try:
                if acl["all_permissions"][0]["inherited"] == True:
                    continue
                aclList.append(
                    list(
                        [
                            acl["group_name"],
                            acl["all_permissions"][0]["permission_level"],
                        ]
                    )
                )
            except KeyError:
                continue
        aclList = [acl for acl in aclList if acl[0] in self.groupL]
        return aclList

    def getACL3(self, acls: dict) -> list:
        aclList = []
        for acl in acls:
            try:
                aclList.append(
                    list(
                        [
                            acl["group_name"],
                            acl["all_permissions"][0]["permission_level"],
                        ]
                    )
                )
            except KeyError:
                continue
        aclList = [acl for acl in aclList if acl[0] in self.groupL]
        return aclList

    def getACL2(self, acls: dict) -> list:
        aclList = []
        for acl in acls:
            try:
                l = []
                for k, v in acl.items():
                    l.append(v)
                aclList.append(l)
            except KeyError:
                continue
        for acl in aclList:
            if acl[0] in self.groupL:
                return aclList
        return {}

    def getSingleClusterACL(self, clusterId):
        if self.verbose:
            print(f"[Verbose] Getting cluster permissions for cluster {clusterId}")
        resCPerm = requests.get(
            f"{self.workspace_url}/api/2.0/preview/permissions/clusters/{clusterId}",
            headers=self.headers,
        )
        if resCPerm.status_code == 404:
            print(f"Error: cluster ACL not enabled for the cluster: {clusterId}")
            return None
        resCPermJson = resCPerm.json()
        aclList = self.getACL(resCPermJson["access_control_list"])
        if len(aclList) == 0:
            return None
        return (clusterId, aclList)

    def getAllClustersACL(self) -> dict:
        print("Performing cluster inventory...")
        try:
            resC = requests.get(
                f"{self.workspace_url}/api/2.0/clusters/list", headers=self.headers
            )
            resCJson = resC.json()
            clusterPerm = {}
            if len(resCJson) == 0:
                return {}
            print(f"Scanning permissions of {len(resCJson['clusters'])} clusters.")
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                future_to_cluster = [
                    executor.submit(self.getSingleClusterACL, c["cluster_id"])
                    for c in resCJson["clusters"]
                ]
                for future in concurrent.futures.as_completed(future_to_cluster):
                    result = future.result()
                    if result is not None:
                        clusterPerm[result[0]] = result[1]
            return clusterPerm
        except Exception as e:
            print(f"error in retrieving cluster permission: {e}")

    def getSingleClusterPolicyACL(self, policyId):
        if self.verbose:
            print(f"[Verbose] Getting policy permissions for {policyId}")
        resCPPerm = requests.get(
            f"{self.workspace_url}/api/2.0/preview/permissions/cluster-policies/{policyId}",
            headers=self.headers,
        )
        if resCPPerm.status_code == 404:
            print(
                f"Error: cluster policy feature is not enabled for policy: {policyId}"
            )
            return None
        resCPPermJson = resCPPerm.json()
        aclList = self.getACL(resCPPermJson["access_control_list"])
        if len(aclList) == 0:
            return None
        return (policyId, aclList)

    def getAllClusterPolicyACL(self) -> dict:
        print("Performing cluster policy inventory...")
        try:
            resCP = requests.get(
                f"{self.workspace_url}/api/2.0/policies/clusters/list",
                headers=self.headers,
            )
            resCPJson = resCP.json()
            if resCPJson["total_count"] == 0:
                print("No cluster policies defined.")
                return {}
            print(
                f"Scanning permissions of {len(resCPJson['policies'])} cluster policies."
            )
            clusterPolicyPerm = {}
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                future_to_cluster = [
                    executor.submit(self.getSingleClusterPolicyACL, c["policy_id"])
                    for c in resCPJson["policies"]
                ]
                for future in concurrent.futures.as_completed(future_to_cluster):
                    result = future.result()
                    if result is not None:
                        clusterPolicyPerm[result[0]] = result[1]
            return clusterPolicyPerm
        except Exception as e:
            print(f"Error in retrieving cluster policy permission: {e}")

    def getSingleWarehouseACL(self, warehouseId):
        if self.verbose:
            print(
                f"[Verbose] Getting warehouse permissions for warehouse {warehouseId}"
            )
        resWPerm = requests.get(
            f"{self.workspace_url}/api/2.0/preview/permissions/sql/warehouses/{warehouseId}",
            headers=self.headers,
        )
        if resWPerm.status_code == 404:
            print(f"Error: warehouse ACL not enabled for the warehouse: {warehouseId}")
            return None
        resWPermJson = resWPerm.json()
        aclList = self.getACL(resWPermJson["access_control_list"])
        if len(aclList) == 0:
            return None
        return (warehouseId, aclList)

    def getAllWarehouseACL(self) -> dict:
        print("Performing warehouse inventory ...")
        try:
            resW = requests.get(
                f"{self.workspace_url}/api/2.0/sql/warehouses", headers=self.headers
            )
            resWJson = resW.json()
            warehousePerm = {}
            if len(resWJson) == 0:
                return {}
            print(f"Scanning permissions of {len(resWJson['warehouses'])} warehouses.")
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                future_to_warehouse = [
                    executor.submit(self.getSingleWarehouseACL, w["id"])
                    for w in resWJson["warehouses"]
                ]
                for future in concurrent.futures.as_completed(future_to_warehouse):
                    result = future.result()
                    if result is not None:
                        warehousePerm[result[0]] = result[1]
            return warehousePerm
        except Exception as e:
            print(f"error in retrieving warehouse permission: {e}")

    def getAllDashboardACL(self, verbose=False) -> dict:
        print("Performing dashboard inventory ...")
        try:
            resD = requests.get(
                f"{self.workspace_url}/api/2.0/preview/sql/dashboards",
                headers=self.headers,
            )
            resDJson = resD.json()
            pages = math.ceil(resDJson["count"] / resDJson["page_size"])

            dashboardPerm = {}
            for pg in range(1, pages + 1):
                if self.verbose:
                    print(f"[Verbose] Requesting dashboard page {pg}...")
                resD = requests.get(
                    f"{self.workspace_url}/api/2.0/preview/sql/dashboards?page={str(pg)}",
                    headers=self.headers,
                )
                resDJson = resD.json()
                results = resDJson["results"]
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.numThreads
                ) as executor:
                    future_dashboard_perms = {
                        executor.submit(
                            self.getSingleDashboardACL, dashboard["id"]
                        ): dashboard["id"]
                        for dashboard in results
                    }
                    for future in concurrent.futures.as_completed(
                        future_dashboard_perms
                    ):
                        dashboard_id = future_dashboard_perms[future]
                        try:
                            result = future.result()
                            if len(result) > 0:
                                dashboardPerm[dashboard_id] = result
                        except Exception as e:
                            print(
                                f"Error in retrieving dashboard permission for dashboard {dashboard_id}: {e}"
                            )
            return dashboardPerm

        except Exception as e:
            print(f"Error in retrieving dashboard permission: {e}")
            raise e

    # this request sometimes fails so we wrap in retry loop
    def getSingleDashboardACL(self, dashboardId) -> list:
        RETRY_LIMIT = 3
        RETRY_DELAY = 500 / 1000  # 500 ms
        retry_count = 0
        while retry_count < RETRY_LIMIT:
            if retry_count > 0:
                time.sleep(RETRY_DELAY)
            if self.verbose:
                print(
                    f"[Verbose] Requesting dashboard id {dashboardId}. retry_count={retry_count}"
                )
            resDPerm = requests.get(
                f"{self.workspace_url}/api/2.0/preview/sql/permissions/dashboards/{dashboardId}",
                headers=self.headers,
            )
            if resDPerm.status_code != 200:
                retry_count += 1
                continue
            try:
                resDPermJson = resDPerm.json()
                aclList = resDPermJson["access_control_list"]
                dashboard_acl = []
                if len(aclList) > 0:
                    for acl in aclList:
                        try:
                            if acl["group_name"] in self.groupL:
                                dashboard_acl = aclList
                                break
                        except KeyError:
                            continue
                return dashboard_acl
            except KeyError:
                retry_count += 1
                continue
        print(
            f"ERROR: Retry limit of {RETRY_LIMIT} exceeded requesting dashboard id {dashboardId}"
        )
        return []  # if retry limit exceeded, return empty list

    def getAllQueriesACL(self, verbose=False) -> dict:
        print("Performing query inventory ...")
        try:
            resQ = requests.get(
                f"{self.workspace_url}/api/2.0/preview/sql/queries",
                headers=self.headers,
            )
            resQJson = resQ.json()
            pages = math.ceil(resQJson["count"] / resQJson["page_size"])

            queryPerm = {}
            for pg in range(1, pages + 1):
                if self.verbose:
                    print(f"[Verbose] Requesting query page {pg}...")
                resQ = requests.get(
                    f"{self.workspace_url}/api/2.0/preview/sql/queries?page={str(pg)}",
                    headers=self.headers,
                )
                resQJson = resQ.json()
                results = resQJson["results"]
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.numThreads
                ) as executor:
                    future_query_perms = {
                        executor.submit(self.getSingleQueryACL, query["id"]): query[
                            "id"
                        ]
                        for query in results
                    }
                    for future in concurrent.futures.as_completed(future_query_perms):
                        query_id = future_query_perms[future]
                        try:
                            result = future.result()
                            if len(result) > 0:
                                queryPerm[query_id] = result
                        except Exception as e:
                            print(
                                f"Error in retrieving query permission for query {query_id}: {e}"
                            )
            return queryPerm

        except Exception as e:
            print(f"Error in retrieving query permission: {e}")
            raise e

    # this request sometimes fails so we wrap in retry loop
    def getSingleQueryACL(self, queryId) -> list:
        RETRY_LIMIT = 3
        RETRY_DELAY = 500 / 1000  # 500 ms
        retry_count = 0
        while retry_count < RETRY_LIMIT:
            if retry_count > 0:
                time.sleep(RETRY_DELAY)
            if self.verbose:
                print(
                    f"[Verbose] Requesting query id {queryId}. retry_count={retry_count}"
                )
            resQPerm = requests.get(
                f"{self.workspace_url}/api/2.0/preview/sql/permissions/queries/{queryId}",
                headers=self.headers,
            )
            if resQPerm.status_code != 200:
                retry_count += 1
                continue
            try:
                resQPermJson = resQPerm.json()
                aclList = resQPermJson["access_control_list"]
                query_acl = []
                if len(aclList) > 0:
                    for acl in aclList:
                        try:
                            if acl["group_name"] in self.groupL:
                                query_acl = aclList
                                break
                        except KeyError:
                            continue
                return query_acl
            except KeyError:
                retry_count += 1
                continue
        print(
            f"ERROR: Retry limit of {RETRY_LIMIT} exceeded requesting query id {queryId}"
        )
        return []  # if retry limit exceeded, return empty list

    def getAlertsACL(self) -> dict:
        try:
            resA = requests.get(
                f"{self.workspace_url}/api/2.0/preview/sql/alerts", headers=self.headers
            )
            resAJson = resA.json()
            alertPerm = {}
            for c in resAJson:
                alertId = c["id"]
                resAPerm = requests.get(
                    f"{self.workspace_url}/api/2.0/preview/sql/permissions/alerts/{alertId}",
                    headers=self.headers,
                )
                if resAPerm.status_code == 404:
                    print(f"feature not enabled for this tier")
                    continue
                resAPermJson = resAPerm.json()
                aclList = resAPermJson["access_control_list"]
                if len(aclList) == 0:
                    continue
                for acl in aclList:
                    try:
                        if acl["group_name"] in self.groupL:
                            alertPerm[alertId] = aclList
                            break
                    except KeyError:
                        continue
            return alertPerm

        except Exception as e:
            print(f"error in retrieving alerts permission: {e}")

    def getPasswordACL(self) -> dict:
        try:
            if self.cloud != "AWS":
                return
            resP = requests.get(
                f"{self.workspace_url}/api/2.0/preview/permissions/authorization/passwords",
                headers=self.headers,
            )
            resPJson = resP.json()
            if len(resPJson) < 3:
                print("No password acls defined.")
                return {}

            passwordPerm = {}
            passwordPerm["passwords"] = self.getACL(resPJson["access_control_list"])
            return passwordPerm
        except Exception as e:
            print(f"error in retrieving password  permission: {e}")

    def getPoolACL(self) -> dict:
        try:
            resIP = requests.get(
                f"{self.workspace_url}/api/2.0/instance-pools/list",
                headers=self.headers,
            )
            resIPJson = resIP.json()
            if len(resIPJson) == 0:
                print("No Instance Pools defined.")
                return {}
            instancePoolPerm = {}
            for c in resIPJson["instance_pools"]:
                instancePID = c["instance_pool_id"]
                resIPPerm = requests.get(
                    f"{self.workspace_url}/api/2.0/preview/permissions/instance-pools/{instancePID}",
                    headers=self.headers,
                )
                if resIPPerm.status_code == 404:
                    print(f"feature not enabled for this tier")
                    continue
                resIPPermJson = resIPPerm.json()
                aclList = self.getACL(resIPPermJson["access_control_list"])
                if len(aclList) == 0:
                    continue
                instancePoolPerm[instancePID] = aclList
            return instancePoolPerm
        except Exception as e:
            print(f"error in retrieving Instance Pool permission: {e}")

    def getAllJobACL(self) -> dict:
        print("Running job ACL inventory ...")
        try:
            jobPerm = {}
            offset = 0
            limit = 25  # 25 is the max before the api complains
            while True:
                # Query next page
                if self.verbose:
                    print(
                        f"[Verbose] Retrieving jobs page offset={offset}, limit={limit}"
                    )
                resJob = requests.get(
                    f"{self.workspace_url}/api/2.1/jobs/list?limit={str(limit)}&offset={str(offset)}",
                    headers=self.headers,
                )
                resJobJson = resJob.json()

                if resJobJson["has_more"] == False and len(resJobJson) == 1:
                    print("Finished listing jobs")
                    break

                # Grab job IDs and parallel map over them to get all ACLs
                jobIDs = [c["job_id"] for c in resJobJson["jobs"]]
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    results = executor.map(self.getSingleJobACL, jobIDs)
                    for result in results:
                        if result is not None:
                            jobPerm[result[0]] = result[1]
                # Check for finish?
                if not resJobJson["has_more"]:
                    break
                offset += limit
            return jobPerm
        except Exception as e:
            print(f"error in retrieving job permissions: {e}")

    def getSingleJobACL(self, jobID):
        try:
            resJobPerm = requests.get(
                f"{self.workspace_url}/api/2.0/permissions/jobs/{jobID}",
                headers=self.headers,
            )
            if resJobPerm.status_code == 404:
                print(f"feature not enabled for this tier")
                return None
            resJobPermJson = resJobPerm.json()
            aclList = self.getACL(resJobPermJson["access_control_list"])
            if len(aclList) == 0:
                return None
            return (jobID, aclList)
        except Exception as e:
            print(f"error in retrieving permission for job {jobID}: {e}")
            return None

    def getExperimentACL(self) -> dict:
        try:
            nextPageToken = ""
            expPerm = {}
            while True:
                data = {}
                data = {"max_results": 100}
                if nextPageToken != "":
                    data = {"page_token": nextPageToken, "max_results": "100"}

                resExp = requests.get(
                    f"{self.workspace_url}/api/2.0/mlflow/experiments/list",
                    headers=self.headers,
                    data=json.dumps(data),
                )
                resExpJson = resExp.json()
                if len(resExpJson) == 0:
                    print("No experiments available")
                    return {}
                for c in resExpJson["experiments"]:
                    expID = c["experiment_id"]
                    # print(c)

                    for k in c["tags"]:
                        if k["key"] == "mlflow.experimentType":
                            if k["value"] == "NOTEBOOK":
                                # print('notebook')
                                resExpPerm = requests.get(
                                    f"{self.workspace_url}/api/2.0/permissions/notebooks/{expID}",
                                    headers=self.headers,
                                )
                            else:
                                # print('experiment')
                                resExpPerm = requests.get(
                                    f"{self.workspace_url}/api/2.0/permissions/experiments/{expID}",
                                    headers=self.headers,
                                )
                    # resExpPerm=requests.get(f"{self.workspace_url}/api/2.0/permissions/experiments/{expID}", headers=self.headers)
                    if resExpPerm.status_code == 404:
                        print(f"feature not enabled for this tier")
                        continue
                    resExpPermJson = resExpPerm.json()
                    if resExpPerm.status_code != 200:
                        print(f"unable to get permission for experiment {expID}")
                        continue
                    aclList = self.getACL(resExpPermJson["access_control_list"])
                    if len(aclList) == 0:
                        continue

                    expPerm[expID] = aclList
                try:
                    nextPageToken = resExpJson["next_page_token"]
                    # break
                except KeyError:
                    break
            return expPerm
        except Exception as e:
            print(f"error in retrieving experiment permission: {e}")

    def getModelACL(self) -> dict:
        try:
            nextPageToken = ""
            expPerm = {}
            while True:
                data = {}
                data = {"max_results": 20}
                if nextPageToken != "":
                    data = {"page_token": nextPageToken}
                resModel = requests.get(
                    f"{self.workspace_url}/api/2.0/mlflow/registered-models/list",
                    headers=self.headers,
                    data=json.dumps(data),
                )
                resModelJson = resModel.json()
                if len(resModelJson) == 0:
                    print("No models available")
                    return {}
                modelPerm = {}
                for c in resModelJson["registered_models"]:
                    modelName = c["name"]
                    param = {"name": modelName}
                    modIDRes = requests.get(
                        f"{self.workspace_url}/api/2.0/mlflow/databricks/registered-models/get",
                        headers=self.headers,
                        data=json.dumps(param),
                    )
                    modelID = modIDRes.json()["registered_model_databricks"]["id"]
                    resModelPerm = requests.get(
                        f"{self.workspace_url}/api/2.0/permissions/registered-models/{modelID}",
                        headers=self.headers,
                    )
                    if resModelPerm.status_code == 404:
                        print(f"feature not enabled for this tier")
                        continue
                    resModelPermJson = resModelPerm.json()
                    aclList = self.getACL(resModelPermJson["access_control_list"])
                    if len(aclList) == 0:
                        continue
                    modelPerm[modelID] = aclList
                try:
                    nextPageToken = resModelJson["next_page_token"]
                    # break
                except KeyError:
                    break
            return modelPerm
        except Exception as e:
            print(f"error in retrieving model permission: {e}")

    def getDLTACL(self) -> dict:
        try:
            nextPageToken = ""
            dltPerm = {}
            while True:
                data = {}
                data = {"max_results": 20}
                if nextPageToken != "":
                    data = {"page_token": nextPageToken}
                resDlt = requests.get(
                    f"{self.workspace_url}/api/2.0/pipelines",
                    headers=self.headers,
                    data=json.dumps(data),
                )
                resDltJson = resDlt.json()
                if len(resDltJson) == 0:
                    print("No dlt pipelines available")
                    return {}
                for c in resDltJson["statuses"]:
                    dltID = c["pipeline_id"]
                    resDltPerm = requests.get(
                        f"{self.workspace_url}/api/2.0/permissions/pipelines/{dltID}",
                        headers=self.headers,
                    )
                    if resDltPerm.status_code == 404:
                        print(f"feature not enabled for this tier")
                        continue
                    resDltPermJson = resDltPerm.json()
                    aclList = self.getACL(resDltPermJson["access_control_list"])
                    if len(aclList) == 0:
                        continue
                    dltPerm[dltID] = aclList
                try:
                    nextPageToken = resDltJson["next_page_token"]
                    # break
                except KeyError:
                    break

            return dltPerm
        except Exception as e:
            print(f"error in retrieving dlt pipelines permission: {e}")

    def getRecursiveFolderList(self, path: str) -> dict:
        print(f"Getting directory structure starting with root path: {path} ...")

        self.folderList.clear()
        self.notebookList.clear()
        self.fileList.clear()
        remaining_dirs = [path]
        depth = 0
        while remaining_dirs:
            if self.verbose:
                print(f"[Verbose] Requesting file list for Depth {depth} Path: {path}")
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                futuresMap = {
                    executor.submit(self.getSingleFolderList, dir_path, depth): dir_path
                    for dir_path in remaining_dirs
                }
                for future in concurrent.futures.as_completed(futuresMap):
                    dir_path = futuresMap[future]
                    res = future.result()
                    if res:
                        dir_path2, folders, notebooks, files = res
                        if dir_path2 != dir_path:
                            print(
                                f"ERROR: got WRONG RESULT from future: sent: {dir_path} recieved: {dir_path2}"
                            )
                            remaining_dirs.remove(dir_path2)
                            # todo: what??
                        else:
                            self.folderList.update(folders)
                            self.notebookList.update(notebooks)
                            self.fileList.update(files)
                            remaining_dirs.extend(
                                dir_path for dir_path in folders.values()
                            )
                    else:
                        print(f"ERROR: one of the futurue results was None: {dir_path}")
                    remaining_dirs.remove(dir_path)
            depth = depth + 1

        return (self.folderList, self.notebookList, self.fileList)

    def getSingleFolderList(self, path: str, depth: int) -> dict:
        MAX_RETRY = 5
        RETRY_DELAY = 500 / 1000
        retry_count = 0
        lastError = ""
        while retry_count < MAX_RETRY:
            # Give some time for the server to recover
            if retry_count > 0:
                time.sleep(RETRY_DELAY)
                # print(f'[ERROR] retrying folder list for folder {path}.')
            if self.verbose:
                print(
                    f"[Verbose] Requesting file list for Depth {depth} Retry {retry_count} Path: {path}"
                )
            retry_count = retry_count + 1
            try:
                data = {"path": path}
                resFolder = requests.get(
                    f"{self.workspace_url}/api/2.0/workspace/list",
                    headers=self.headers,
                    data=json.dumps(data),
                )
                if resFolder.status_code == 403:
                    print(
                        f"[ERROR] status code 403 permission denied to read folder {path}."
                    )
                    return (path, {}, {})
                if resFolder.status_code != 200:
                    print(
                        f"[ERROR] bad status code for folder {path}. code: {resFolder.status_code}"
                    )
                    continue
                resFolderJson = resFolder.json()

                subFolders = {}
                notebooks = {}
                files = {}
                if len(resFolderJson) == 0:
                    return (path, subFolders, notebooks, files)

                for c in resFolderJson["objects"]:
                    if (
                        c["object_type"] == "DIRECTORY"
                        and c["path"].startswith("/Shared") == False
                        and c["path"].endswith("/Trash") == False
                    ):
                        subFolders[c["object_id"]] = c["path"]
                    elif (
                        c["object_type"] == "NOTEBOOK"
                        and c["path"].startswith("/Repos") == False
                        and c["path"].startswith("/Shared") == False
                    ):
                        notebooks[c["object_id"]] = c["path"]
                    elif (
                        c["object_type"] == "FILE"
                        and c["path"].startswith("/Repos") == False
                        and c["path"].startswith("/Shared") == False
                    ):
                        files[c["object_id"]] = c["path"]
                return (path, subFolders, notebooks, files)

            except Exception as e:
                lastError = e
                continue
        print(
            f"[ERROR] retry limit ({MAX_RETRY}) limit exceeded while retrieving path {path}. last err: {lastError}."
        )
        return (path, {}, {}, {})

    def getFoldersNotebookACL(self, rootPath="/") -> list:
        print("Performing folders and notebook inventory ...")
        try:
            # Get folder list
            self.getRecursiveFolderList(rootPath)

            # Collect folder IDs, ignoring suffix /Trash to avoid useless errors. /Repos and /Shared are ignored at the folder list level
            folder_ids = [
                folder_id
                for folder_id in self.folderList.keys()
                if not self.folderList[folder_id].endswith("/Trash")
            ]

            # Get folder permissions in parallel
            folder_results = {}
            currentFolderCount = 0
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                folder_futures = {
                    executor.submit(
                        requests.get,
                        f"{self.workspace_url}/api/2.0/permissions/directories/{folder_id}",
                        headers=self.headers,
                    ): folder_id
                    for folder_id in folder_ids
                }
                print(
                    f"Awaiting parallel permission requests for {len(folder_futures)} folders ..."
                )
                for future in concurrent.futures.as_completed(folder_futures):
                    folder_id = folder_futures[future]
                    try:
                        resFolderPerm = future.result()
                        currentFolderCount += 1
                        if resFolderPerm.status_code == 404:
                            print(f"feature not enabled for this tier")
                            continue
                        if resFolderPerm.status_code == 403:
                            print(
                                "Error retrieving permission for "
                                + self.folderList[folder_id]
                                + " "
                                + resFolderPerm.json()["message"]
                            )
                            continue
                        resFolderPermJson = resFolderPerm.json()
                        try:
                            aclList = self.getACL(
                                resFolderPermJson["access_control_list"]
                            )
                        except Exception as e:
                            print(f"error in retrieving folder details: {e}")
                        if currentFolderCount % 1000 == 0:
                            print(f"Completed ACL for {currentFolderCount} folders")
                        if len(aclList) == 0:
                            continue
                        folder_results[folder_id] = aclList

                    except Exception as e:
                        print(f"error in retrieving folder permission: {e}")

            # Get notebook permissions in parallel
            notebook_results = {}
            currentNotebookCount = 0
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                notebook_futures = {
                    executor.submit(
                        requests.get,
                        f"{self.workspace_url}/api/2.0/permissions/notebooks/{notebook_id}",
                        headers=self.headers,
                    ): notebook_id
                    for notebook_id in self.notebookList.keys()
                }
                print(
                    f"Awaiting parallel permission requests for {len(notebook_futures)} notebooks ..."
                )
                for future in concurrent.futures.as_completed(notebook_futures):
                    notebook_id = notebook_futures[future]
                    try:
                        resNotebookPerm = future.result()
                        currentNotebookCount += 1
                        if resNotebookPerm.status_code == 404:
                            print(f"feature not enabled for this tier")
                            continue
                        if resNotebookPerm.status_code == 403:
                            print(
                                "Error retrieving permission for "
                                + self.notebookList[notebook_id]
                                + " "
                                + resNotebookPerm.json()["message"]
                            )
                            continue
                        resNotebookPermJson = resNotebookPerm.json()
                        try:
                            aclList = self.getACL(
                                resNotebookPermJson["access_control_list"]
                            )
                        except Exception as e:
                            print(f"error in retrieving notebook details: {e}")
                        if currentNotebookCount % 1000 == 0:
                            print(f"Completed ACL for {currentNotebookCount} notebooks")
                        if len(aclList) == 0:
                            continue
                        notebook_results[notebook_id] = aclList

                    except Exception as e:
                        print(f"error in retrieving notebook permission: {e}")

            # Get file permissions in parallel
            file_results = {}
            currentFileCount = 0
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                file_futures = {
                    executor.submit(
                        requests.get,
                        f"{self.workspace_url}/api/2.0/permissions/files/{file_id}",
                        headers=self.headers,
                    ): file_id
                    for file_id in self.fileList.keys()
                }
                print(
                    f"Awaiting parallel permission requests for {len(file_futures)} files ..."
                )
                for future in concurrent.futures.as_completed(file_futures):
                    file_id = file_futures[future]
                    try:
                        resFilePerm = future.result()
                        currentFileCount += 1
                        if resFilePerm.status_code == 404:
                            print(f"feature not enabled for this tier")
                            continue
                        if resFilePerm.status_code == 403:
                            print(
                                "Error retrieving permission for "
                                + self.fileList[file_id]
                                + " "
                                + resFilePerm.json()["message"]
                            )
                            continue
                        resFilePermJson = resFilePerm.json()
                        try:
                            aclList = self.getACL(
                                resFilePermJson["access_control_list"]
                            )
                        except Exception as e:
                            print(f"error in retrieving file details: {e}")
                        if currentFileCount % 1000 == 0:
                            print(f"Completed ACL for {currentFileCount} notebooks")
                        if len(aclList) == 0:
                            continue
                        file_results[file_id] = aclList

                    except Exception as e:
                        print(f"error in retrieving file permission: {e}")

            return folder_results, notebook_results, file_results
        except Exception as e:
            print(f"error in retrieving folder and notebook permissions: {e}")

    def getRepoACL(self) -> dict:
        try:
            nextPageToken = ""
            repoPerm = {}
            while True:
                data = {}
                data = {"max_results": 20}
                if nextPageToken != "":
                    data = {"next_page_token": nextPageToken}
                resRepo = requests.get(
                    f"{self.workspace_url}/api/2.0/repos",
                    headers=self.headers,
                    data=json.dumps(data),
                )
                resRepoJson = resRepo.json()
                if len(resRepoJson) == 0:
                    print("No repos available")
                    return {}
                for c in resRepoJson["repos"]:
                    repoID = c["id"]
                    resRepoPerm = requests.get(
                        f"{self.workspace_url}/api/2.0/permissions/repos/{repoID}",
                        headers=self.headers,
                    )
                    if resRepoPerm.status_code == 404:
                        print(f"feature not enabled for this tier")
                        continue
                    resRepoPermJson = resRepoPerm.json()
                    aclList = self.getACL3(resRepoPermJson["access_control_list"])
                    if len(aclList) == 0:
                        continue
                    repoPerm[repoID] = aclList
                try:
                    nextPageToken = resRepoJson["next_page_token"]
                except KeyError:
                    break

            return repoPerm
        except Exception as e:
            print(f"error in retrieving repos permission: {e}")

    def getTokenACL(self) -> dict:
        try:
            tokenPerm = {}
            resTokenPerm = requests.get(
                f"{self.workspace_url}/api/2.0/preview/permissions/authorization/tokens",
                headers=self.headers,
            )
            if resTokenPerm.status_code == 404:
                print(f"feature not enabled for this tier")
                return {}
            resTokenPermJson = resTokenPerm.json()
            aclList = []
            for acl in resTokenPermJson["access_control_list"]:
                try:
                    if acl["all_permissions"][0]["inherited"] == True:
                        continue
                    aclList.append(
                        list(
                            [
                                acl["group_name"],
                                acl["all_permissions"][0]["permission_level"],
                            ]
                        )
                    )
                except KeyError:
                    continue
            aclList = [acl for acl in aclList if acl[0] in self.groupL]
            tokenPerm["tokens"] = aclList
            return tokenPerm
        except Exception as e:
            print(f"error in retrieving Token permission: {e}")
            return {}

    def getSecretScoppeACL(self) -> dict:
        try:
            resSScope = requests.get(
                f"{self.workspace_url}/api/2.0/secrets/scopes/list",
                headers=self.headers,
            )
            resSScopeJson = resSScope.json()
            if len(resSScopeJson) == 0:
                print("No secret scopes defined.")
                return {}

            secretScopePerm = {}
            for c in resSScopeJson["scopes"]:
                scopeName = c["name"]
                data = {"scope": scopeName}
                resSSPerm = requests.get(
                    f"{self.workspace_url}/api/2.0/secrets/acls/list/",
                    headers=self.headers,
                    data=json.dumps(data),
                )

                if resSSPerm.status_code == 404:
                    print(f"feature not enabled for this tier")
                    continue
                if resSSPerm.status_code != 200:
                    print(
                        f"Error retrieving ACL for Secret Scope: {scopeName}. HTTP Status Code {resSSPerm.status_code}"
                    )
                    continue

                resSSPermJson = resSSPerm.json()
                if not "items" in resSSPermJson:
                    # print(f'ACL for Secret Scope  {scopeName} missing "items" key. Contents:\n{resSSPermJson}\nSkipping...')
                    # This seems to be expected behaviour if there are no ACLs, silently ignore
                    continue

                aclList = []
                for acl in resSSPermJson["items"]:
                    try:
                        if acl["principal"] in self.groupL:
                            aclList.append(list([acl["principal"], acl["permission"]]))
                    except KeyError:
                        continue
                if len(aclList) == 0:
                    continue
                secretScopePerm[scopeName] = aclList

            return secretScopePerm
        except Exception as e:
            print(f"error in retrieving Secret Scope permission: {e}")

    def updateGroupEntitlements(self, groupEntitlements: dict, level: str):
        try:
            for group_id, etl in groupEntitlements.items():
                entitlementList = []
                if level == "Workspace":
                    groupId = self.groupWSGNameDict[
                        "db-temp-" + self.groupIdDict[group_id]
                    ]
                else:  # Account, aka temp group, must discard db-temp- (8 chars)
                    groupId = self.accountGroups_lower[
                        self.groupIdDict[group_id][8:].casefold()
                    ]
                # print(groupId)
                for e in etl:
                    entitlementList.append({"value": e})
                entitlements = {
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [
                        {"op": "add", "path": "entitlements", "value": entitlementList}
                    ],
                }
                resPatch = requests.patch(
                    f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{groupId}",
                    headers=self.headers,
                    data=json.dumps(entitlements),
                )
        except Exception as e:
            print(f"error applying entitiement for group id: {group_id}.")

    def updateGroupRoles(self, level: str):
        try:
            for group_id, roles in self.groupRoles.items():
                roleList = []
                if level == "Workspace":
                    groupId = self.groupWSGNameDict[
                        "db-temp-" + self.groupIdDict[group_id]
                    ]
                else:  # Account, aka temp group, must discard db-temp- (8 chars)
                    groupId = self.accountGroups_lower[
                        self.groupIdDict[group_id][8:].casefold()
                    ]
                for e in roles:
                    roleList.append({"value": e})
                instanceProfileRoles = {
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [{"op": "add", "path": "roles", "value": roleList}],
                }
                resPatch = requests.patch(
                    f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{groupId}",
                    headers=self.headers,
                    data=json.dumps(instanceProfileRoles),
                )
        except Exception as e:
            print(f"error applying role for group id: {group_id}.")

    def updateGroupPermission(self, object: str, groupPermission: dict, level: str):
        try:
            suffix = ""
            for object_id, aclList in groupPermission.items():
                dataAcl = []
                for acl in aclList:
                    if level == "Workspace":
                        gName = "db-temp-" + acl[0]
                    elif level == "Account":
                        gName = acl[0][8:]
                    dataAcl.append({"group_name": gName, "permission_level": acl[1]})
                data = {"access_control_list": dataAcl}
                resAppPerm = requests.patch(
                    f"{self.workspace_url}/api/2.0/preview/permissions/{object}/{object_id}",
                    headers=self.headers,
                    data=json.dumps(data),
                )
        except Exception as e:
            print(f"Error setting permission for {object} {object_id}. {e} ")

    def updateGroup2Permission(self, object: str, groupPermission: dict, level: str):
        try:
            for object_id, aclList in groupPermission.items():
                addUser = True
                dataAcl = []
                for acl in aclList:
                    try:
                        gName = acl["group_name"]
                        if gName == "ADMIN" and acl["permission_level"] != "CAN_MANAGE":
                            dataAcl.append(
                                {"group_name": gName, "permission_level": "CAN_MANAGE"}
                            )
                        if level == "Workspace":
                            if acl["group_name"] in self.WorkspaceGroupNames:
                                gName = "db-temp-" + acl["group_name"]
                                dataAcl.append(
                                    {
                                        "group_name": gName,
                                        "permission_level": acl["permission_level"],
                                    }
                                )
                        elif level == "Account":
                            if acl["group_name"] in self.TempGroupNames:
                                gName = acl["group_name"][8:]
                        else:
                            gName = acl["group_name"]
                        dataAcl.append(acl)
                    except KeyError:
                        dataAcl.append(acl)
                        continue
                data = {"access_control_list": dataAcl}
                resAppPerm = requests.post(
                    f"{self.workspace_url}/api/2.0/preview/sql/permissions/{object}/{object_id}",
                    headers=self.headers,
                    data=json.dumps(data),
                )
        except Exception as e:
            print(f"Error setting permission for {object} {object_id}. {e} ")

    def updateSecretPermission(self, secretPermission: dict, level: str):
        try:
            suffix = ""
            for object_id, aclList in secretPermission.items():
                dataAcl = []
                for acl in aclList:
                    if level == "Workspace":
                        gName = "db-temp-" + acl[0]
                    elif level == "Account":
                        gName = acl[0][8:]
                    data = {
                        "scope": object_id,
                        "principal": gName,
                        "permission": acl[1],
                    }
                    resAppPerm = requests.post(
                        f"{self.workspace_url}/api/2.0/secrets/acls/put",
                        headers=self.headers,
                        data=json.dumps(data),
                    )
        except Exception as e:
            print(f"Error setting permission for scope {object_id}. {e} ")

    def runVerboseSql(self, queryString):
        if self.verbose:
            print(f"[Verbose] SQL: {queryString}")
        return self.spark.sql(queryString)

    def getGrantsOnObjects(self, database_name: str, object_type: str, object_key: str):
        try:
            if object_type in [
                "CATALOG",
                "ANY FILE",
                "ANONYMOUS FUNCTION",
            ]:  # without object key
                grants_df = (
                    self.spark.sql(f"SHOW GRANT ON {object_type}")
                    .groupBy("ObjectType", "ObjectKey", "Principal")
                    .agg(collect_set("ActionType").alias("ActionTypes"))
                    .selectExpr(
                        "CAST(NULL AS STRING) AS Database",
                        "Principal",
                        "ActionTypes",
                        "ObjectType",
                        "ObjectKey",
                    )
                )
            else:
                grants_df = (
                    self.spark.sql(f"SHOW GRANT ON {object_type} {object_key}")
                    .filter(col("ObjectType") == f"{object_type}")
                    .groupBy("ObjectType", "ObjectKey", "Principal")
                    .agg(collect_set("ActionType").alias("ActionTypes"))
                    .selectExpr(
                        f"'{database_name}' AS Database",
                        "Principal",
                        "ActionTypes",
                        "ObjectType",
                        "ObjectKey",
                    )
                )
        except Exception as e:
            print(f"Error retrieving grants on object {object_key}. {e}")
            return

        return grants_df

    def getDBACL(self, db: str):
        try:
            aclList = []
            dbdf = self.getGrantsOnObjects(db, "DATABASE", db)
            aclList += dbdf.collect()
            if not self.checkAllDB:
                userListCollect = (
                    dbdf.filter(col("ObjectType") == "DATABASE")
                    .filter(
                        (
                            array_contains(col("ActionTypes"), "USAGE")
                            | array_contains(col("ActionTypes"), "OWN")
                        )
                    )
                    .select(col("Principal"))
                    .collect()
                )
                userList = [p.Principal for p in userListCollect]
                userList = list(set(userList))
                if not self.checkPrincipalInGroupOrMember(userList, db):
                    # print(f'selected groups or members of the groups have no USAGE or OWN permission on database level. Skipping object level permission check for database {db}.')
                    return []

            tables = self.runVerboseSql(
                "show tables in spark_catalog.{}".format(db)
            ).filter(col("isTemporary") == False)
            for table in tables.collect():
                try:
                    tbldf = self.getGrantsOnObjects(
                        db, "TABLE", f"`{table.database}`.`{table.tableName}`"
                    )
                    aclList += tbldf.collect()
                except Exception as e:
                    print(f"error retrieving acl for table {table.tableName}. {e}")

            functions = self.runVerboseSql("show functions in {}".format(db)).filter(
                col("function").startswith("spark_catalog." + db + ".")
            )
            for function in functions.collect():
                try:
                    funcdf = self.getGrantsOnObjects(
                        db, "FUNCTION", f"{function.function}"
                    )
                    aclList += funcdf.collect()
                except Exception as e:
                    print(f"error retrieving acl for function {function.function}. {e}")
            # filter for required groups
            return aclList

        except Exception as e:
            print(f"Error retrieving ACL for database {db}. {e}")

    # check principal given usage permission is group or a member of the group (user or sp)
    def checkPrincipalInGroupOrMember(self, principalList: str, name: str) -> bool:
        for p in principalList:
            if p in self.groupGroupList:
                print(f"Group {p} is given USAGE or OWN permission for {name}.")
                return True
        for p in principalList:
            if p in self.groupUserList:
                print(f"User {p} is given USAGE or OWN permission for {name}.")
                return True
        for p in principalList:
            if p in self.groupSPList:
                print(f"SP {p} is given USAGE or OWN permission for {name}.")
                return True
        return False

    def getTableACLs(self) -> list:
        self.groupUserList = []
        self.groupSPList = []
        self.getRecursiveGroupMember(self.groupMembers)
        self.groupUserList = list(set(self.groupUserList))
        self.groupSPList = list(set(self.groupSPList))
        # ANONYMOUS FUNCTION
        common_df = self.getGrantsOnObjects(None, "ANONYMOUS FUNCTION", None)
        # ANY FILE
        common_df = common_df.unionAll(self.getGrantsOnObjects(None, "ANY FILE", None))
        # CATALOG
        common_df = common_df.unionAll(self.getGrantsOnObjects(None, "CATALOG", None))
        aclList = []
        aclList = common_df.collect()
        # check if any group is given permission at catalog level
        userListCollect = (
            common_df.filter(col("ObjectType") == "CATALOG$")
            .filter(array_contains(col("ActionTypes"), "USAGE"))
            .select(col("Principal"))
            .collect()
        )
        userList = [p.Principal for p in userListCollect]
        userList = list(set(userList))
        if self.checkPrincipalInGroupOrMember(userList, "CATALOG"):
            print(
                f"some groups or members of the group given permission at catalog level, running permission for all databases"
            )
            self.checkAllDB = True
        database_names = []
        dbs = self.spark.sql("show databases").collect()
        totalDBs = len(database_names)
        for db in dbs:
            database_names.append(db.databaseName)
        print(f"Got {len(database_names)} dbs to query")
        # database_names=['aaron_binns','hsdb']
        currentCount = 0
        try:
            # aclList = []
            aclFinalList = []
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.numThreads
            ) as executor:
                future_db = [
                    executor.submit(self.getDBACL, f"`{databaseName}`")
                    for databaseName in database_names
                ]
                for future in concurrent.futures.as_completed(future_db):
                    result = future.result()
                    if result is not None:
                        aclList += result
                    currentCount += 1
                    if currentCount % 100 == 0:
                        print(f"Completed ACL for {currentCount} databases")
            aclFinalList = [acl for acl in aclList if acl.Principal in self.groupL]
        except Exception as e:
            print(f"Error retrieving table acl object permission {e}")
        return aclFinalList

    def generate_table_acls_command(
        self, action_types, object_type, object_key, groupName
    ):
        lines = []
        grant_privs = [
            x for x in action_types if not x.startswith("DENIED_") and x != "OWN"
        ]
        deny_privs = [
            x[len("DENIED_") :]
            for x in action_types
            if x.startswith("DENIED_") and x != "OWN"
        ]
        if grant_privs:
            lines.append(
                f"GRANT {', '.join(grant_privs)} ON {object_type} {object_key} TO `{groupName}`;"
            )
        if deny_privs:
            lines.append(
                f"DENY {', '.join(deny_privs)} ON {object_type} {object_key} TO `{groupName}`;"
            )
        if "OWN" in action_types:
            lines.append(f"ALTER {object_type} {object_key} OWNER TO `{groupName}`;")
        return lines

    def updateDataObjectsPermission(self, aclList: List, level: str):
        try:
            lines = []
            for acl in aclList:
                # if acl.ObjectType!="DATABASE" and acl.ActionType=="USAGE": continue
                if level == "Workspace":
                    gName = "db-temp-" + acl.Principal
                elif level == "Account":
                    gName = acl.Principal[8:]
                if acl.ObjectType == "ANONYMOUS_FUNCTION":
                    lines.extend(
                        self.generate_table_acls_command(
                            acl.ActionTypes, "ANONYMOUS FUNCTION", "", gName
                        )
                    )
                elif acl.ObjectType == "ANY_FILE":
                    lines.extend(
                        self.generate_table_acls_command(
                            acl.ActionTypes, "ANY FILE", "", gName
                        )
                    )
                elif acl.ObjectType == "CATALOG$":
                    lines.extend(
                        self.generate_table_acls_command(
                            acl.ActionTypes, "CATALOG", "", gName
                        )
                    )
                elif acl.ObjectType in ["DATABASE", "TABLE"]:
                    # DATABASE, TABLE, VIEW (view's seem to show up as tables)
                    lines.extend(
                        self.generate_table_acls_command(
                            acl.ActionTypes, acl.ObjectType, acl.ObjectKey, gName
                        )
                    )
                # lines.extend(self.generate_table_acls_command(acl.ActionTypes, acl.ObjectType, acl.ObjectKey, gName))
            for aclQuery in lines:
                # print(aclQuery)
                self.runVerboseSql(aclQuery)
        except Exception as e:
            print(f"Error setting permission, {e} ")

    def setGroupListForMode(self, mode: str):
        print(f"Retrieving group metadata for mode: {mode}")
        if mode == "Workspace":
            self.groupL = self.WorkspaceGroupNames
            self.getGroupObjects(self.groupL)
        elif mode == "Account":
            self.groupL = self.TempGroupNames
            self.getGroupObjects(self.groupL)
        else:
            raise ValueError(
                f"mode {mode} not supported. Valid values are 'Workspace' and 'Account'"
            )

    def clearInventoryCache(self):
        self.lastInventoryRun = None

    def performInventory(self, mode: str, force: bool = False):
        # check if all this should already be cached
        if self.lastInventoryRun == mode and not force:
            self.setGroupListForMode(mode)
            print(f"Skipping inventory for mode = {mode} since already performed.")
            return

        print(
            f"Performing inventory of workspace object permissions. Filtering results by group list for mode: {mode}."
        )
        try:
            self.setGroupListForMode(mode)
            if self.cloud == "AWS":
                print("performing password inventory")
                self.passwordPerm = self.getPasswordACL()

            # These are parallel
            self.clusterPerm = self.getAllClustersACL()
            self.clusterPolicyPerm = self.getAllClusterPolicyACL()
            self.warehousePerm = self.getAllWarehouseACL()
            self.dashboardPerm = self.getAllDashboardACL()  # 5 mins
            self.queryPerm = self.getAllQueriesACL()
            self.jobPerm = self.getAllJobACL()  # 33 mins
            (
                self.folderPerm,
                self.notebookPerm,
                self.filePerm,
            ) = self.getFoldersNotebookACL()

            # These have yet to be parallelized:
            if self.checkTableACL == True:
                print("performing Tabel ACL object inventory")
                self.dataObjectsPerm = self.getTableACLs()

            print("performing alerts inventory")
            self.alertPerm = self.getAlertsACL()
            print("performing instance pools inventory")
            self.instancePoolPerm = self.getPoolACL()
            print("performing experiments inventory")
            self.expPerm = self.getExperimentACL()
            print("performing registered models inventory")
            self.modelPerm = self.getModelACL()
            print("performing DLT inventory")
            self.dltPerm = self.getDLTACL()
            print("performing repos inventory")
            self.repoPerm = self.getRepoACL()
            print("performing token inventory")
            self.tokenPerm = self.getTokenACL()
            print("performing secret scope inventory")
            self.secretScopePerm = self.getSecretScoppeACL()

            self.lastInventoryRun = mode
        except Exception as e:
            print(f" Error creating group inventory, {e}")

    def printInventory(self, printMembers: bool = False):
        print("Displaying Inventory Results -- ACLs of selected groups:")
        print("Group List:")
        print("{:<20} {:<10}".format("Group ID", "Group Name"))
        for key, value in self.groupIdDict.items():
            print("{:<20} {:<10}".format(key, value))

        if printMembers:
            print("Group Members:")
            print("{:<20} {:<100}".format("Group ID", "Group Member"))
            for key, value in self.groupMembers.items():
                print("{:<20} {:<100}".format(key, str(value)))

        print("Group Entitlements:")
        print("{:<20} {:<100}".format("Group ID", "Group Entitlements"))
        for key, value in self.groupEntitlements.items():
            print("{:<20} {:<100}".format(key, str(value)))
        if self.cloud == "AWS":
            print("Group Roles:")
            print("{:<20} {:<100}".format("Group ID", "Group Roles"))
            for key, value in self.groupRoles.items():
                print("{:<20} {:<100}".format(key, str(value)))
            print("Group Passwords:")
            print("{:<20} {:<100}".format("Password", "Group Names"))
            for key, value in self.passwordPerm.items():
                print("{:<20} {:<100}".format(key, str(value)))
        print("Cluster Permission:")
        print("{:<20} {:<100}".format("Cluster ID", "Group Permission"))
        for key, value in self.clusterPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Cluster Policy Permission:")
        print("{:<20} {:<100}".format("Cluster Policy ID", "Group Permission"))
        for key, value in self.clusterPolicyPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Warehouse Permission:")
        print("{:<20} {:<100}".format("SQL Warehouse ID", "Group Permission"))
        for key, value in self.warehousePerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Dashboard Permission:")
        print("{:<20} {:<100}".format("Dashboard ID", "Group Permission"))
        for key, value in self.dashboardPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Query Permission:")
        print("{:<20} {:<100}".format("Query ID", "Group Permission"))
        for key, value in self.queryPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Alerts Permission:")
        print("{:<20} {:<100}".format("Alerts ID", "Group Permission"))
        for key, value in self.alertPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Instance Pool Permission:")
        print("{:<20} {:<100}".format("InstancePool ID", "Group Permission"))
        for key, value in self.instancePoolPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Jobs Permission:")
        print("{:<20} {:<100}".format("Job ID", "Group Permission"))
        for key, value in self.jobPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Experiments Permission:")
        print("{:<20} {:<100}".format("Experiment ID", "Group Permission"))
        for key, value in self.expPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Models Permission:")
        print("{:<20} {:<100}".format("Model ID", "Group Permission"))
        for key, value in self.modelPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Delta Live Tables Permission:")
        print("{:<20} {:<100}".format("Pipeline ID", "Group Permission"))
        for key, value in self.dltPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Repos Permission:")
        print("{:<20} {:<100}".format("Repo ID", "Group Permission"))
        for key, value in self.repoPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Tokens Permission:")
        print("{:<20} {:<100}".format("Token ID", "Group Permission"))
        for key, value in self.tokenPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Secret Scopes Permission:")
        print("{:<20} {:<100}".format("SecretScope ID", "Group Permission"))
        for key, value in self.secretScopePerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Folder  Permission:")
        print("{:<20} {:<100}".format("Folder ID", "Group Permission"))
        for key, value in self.folderPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("Notebook  Permission:")
        print("{:<20} {:<100}".format("Notebook ID", "Group Permission"))
        for key, value in self.notebookPerm.items():
            print("{:<20} {:<100}".format(key, str(value)))
        print("File  Permission:")
        print("{:<20} {:<100}".format("File ID", "Group Permission"))
        for key, value in self.filePerm.items():
            print("{:<20} {:<100}".format(key, str(value)))

        if self.checkTableACL == True:
            print("TableACL  Permission:")
            for item in self.dataObjectsPerm:
                print(item)

    def dryRun(self, mode: str = "Workspace", printMembers: bool = False):
        self.performInventory(mode)
        self.printInventory(printMembers)

    def applyGroupPermission(self, level: str):
        try:
            print("applying group entitlement permissions")
            self.updateGroupEntitlements(self.groupEntitlements, level)
            print("applying cluster permissions")
            self.updateGroupPermission("clusters", self.clusterPerm, level)
            print("applying cluster policy permissions")
            self.updateGroupPermission(
                "cluster-policies", self.clusterPolicyPerm, level
            )
            print("applying warehouse permissions")
            self.updateGroupPermission("sql/warehouses", self.warehousePerm, level)
            print("applying instance pool permissions")
            self.updateGroupPermission("instance-pools", self.instancePoolPerm, level)
            print("applying jobs permissions")
            self.updateGroupPermission("jobs", self.jobPerm, level)
            print("applying experiments permissions")
            self.updateGroupPermission("experiments", self.expPerm, level)
            print("applying model permissions")
            self.updateGroupPermission("registered-models", self.modelPerm, level)
            print("applying DLT permissions")
            self.updateGroupPermission("pipelines", self.dltPerm, level)
            print("applying folders permissions")
            self.updateGroupPermission("directories", self.folderPerm, level)
            print("applying notebooks permissions")
            self.updateGroupPermission("notebooks", self.notebookPerm, level)
            print("applying files permissions")
            self.updateGroupPermission("files", self.filePerm, level)
            print("applying repos permissions")
            self.updateGroupPermission("repos", self.repoPerm, level)
            print("applying token permissions")
            self.updateGroupPermission("authorization", self.tokenPerm, level)
            print("applying secret scope permissions")
            self.updateSecretPermission(self.secretScopePerm, level)
            print("applying dashboard permissions")
            self.updateGroup2Permission("dashboards", self.dashboardPerm, level)
            print("applying query permissions")
            self.updateGroup2Permission("queries", self.queryPerm, level)
            print("applying alerts permissions")
            self.updateGroup2Permission("alerts", self.alertPerm, level)
            if self.cloud == "AWS":
                print("applying password permissions")
                self.updateGroupPermission("authorization", self.passwordPerm, level)
                print("applying instance profile permissions")
                self.updateGroupRoles(level)
            if self.checkTableACL == True:
                print("applying table acl object permissions")
                self.updateDataObjectsPermission(self.dataObjectsPerm, level)

        except Exception as e:
            print(f" Error applying group permission, {e}")

    def validateTempWSGroup(self) -> list:
        try:
            res = requests.get(
                f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups",
                headers=self.headers,
            )
            resJson = res.json()
            WSGGroup = [
                e["displayName"]
                for e in resJson["Resources"]
                if e["meta"]["resourceType"] == "WorkspaceGroup"
            ]
            for g in self.groupL:
                if "db-temp-" + g not in WSGGroup:
                    print(f"temp workspace group db-temp-{g} not present, please check")
                    return 0
            return 1
        except Exception as e:
            print(f"error validating WS group objects : {e}")

    def bulkTryDelete(self, deleteList):
        for g in deleteList:
            gID = self.groupNameDict[g]
            print(f"Attempting to delete group [{gID}] - {g}")
            try:
                res = requests.delete(
                    f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups/{gID}",
                    headers=self.headers,
                )
            except Exception as deleteError:
                print(
                    "ERROR - Failed to delete group [{gID}] - {g}. ErrorMessage: {deleteError}"
                )
                pass
            else:
                print(f"SUCCESS - Deleted group [{gID}] - {g}")

    def persistInventory(self, mode: str):
        try:
            groupType = ""
            if mode == "Workspace":
                print(
                    f"Saving data for workspace groups in {self.inventoryTableName} table."
                )
                groupType = "WorkspaceLocal"
            else:
                print(
                    f"Saving data for workspace temp groups in {self.inventoryTableName} table."
                )
                groupType = "WorkspaceTemp"
            persistList = []
            persistList.append([groupType, "GroupListDict", self.groupIdDict])
            persistList.append([groupType, "GroupMembers", self.groupMembers])
            persistList.append([groupType, "GroupEntitlements", self.groupEntitlements])
            persistList.append([groupType, "GroupRoles", self.groupRoles])
            persistList.append([groupType, "Password", self.passwordPerm])
            persistList.append([groupType, "Cluster", self.clusterPerm])
            persistList.append([groupType, "ClusterPolicy", self.clusterPolicyPerm])
            persistList.append([groupType, "Warehouse", self.warehousePerm])
            persistList.append([groupType, "Dashboard", self.dashboardPerm])
            persistList.append([groupType, "Query", self.queryPerm])
            persistList.append([groupType, "Job", self.jobPerm])
            persistList.append([groupType, "Folder", self.folderPerm])
            persistList.append([groupType, "Notebook", self.notebookPerm])
            persistList.append([groupType, "File", self.filePerm])
            persistList.append([groupType, "Alert", self.alertPerm])
            persistList.append([groupType, "Pool", self.instancePoolPerm])
            persistList.append([groupType, "Experiment", self.expPerm])
            persistList.append([groupType, "Model", self.modelPerm])
            persistList.append([groupType, "DLT", self.dltPerm])
            persistList.append([groupType, "Repo", self.repoPerm])
            persistList.append([groupType, "Token", self.tokenPerm])
            persistList.append([groupType, "Secret", self.secretScopePerm])
            persistColumns = StructType(
                [
                    StructField("GroupType", StringType(), True),
                    StructField("WorkspaceObject", StringType(), True),
                    StructField(
                        "Permission", MapType(StringType(), StringType()), True
                    ),
                ]
            )
            # persistColumns=["GroupType", "WorkspaceObject","Permission"]
            persistDF = self.spark.createDataFrame(
                data=persistList, schema=persistColumns
            )
            # return persistDF
            persistDF.write.format("delta").mode("append").saveAsTable(
                self.inventoryTableName
            )

            if self.checkTableACL:
                tableACLCol = StructType(
                    [
                        StructField("Database", StringType(), True),
                        StructField("Principal", StringType(), True),
                        StructField("ActionTypes", StringType(), True),
                        StructField("ObjectType", StringType(), True),
                        StructField("ObjectKey", StringType(), True),
                    ]
                )

                tableACLDF = self.spark.createDataFrame(
                    data=self.dataObjectsPerm, schema=tableACLCol
                ).withColumn("GroupType", lit(groupType))
                tableACLDF.write.format("delta").mode("append").saveAsTable(
                    self.inventoryTableName + "TableACL"
                )
            print(f"Saved data in {self.inventoryTableName} table.")

        except Exception as e:
            print(f"Error creating delta table to store inventory  : {e}")

    def deleteWorkspaceLocalGroups(self):
        try:
            self.setGroupListForMode("Workspace")
            if self.validateTempWSGroup() == 0:
                print("temp group validation failed, aborting deletion")
                return
            self.bulkTryDelete(self.groupL)
        except Exception as e:
            print(f"Error deleting groups : {e}")

    def deleteTempGroups(self):
        self.setGroupListForMode("Account")
        try:
            self.bulkTryDelete(self.groupL)
        except Exception as e:
            print(f"Error deleting temp groups : {e}")

    def createBackupGroup(self):
        try:
            if self.validateWSGroup() == 0:
                return
            self.performInventory("Workspace")
            self.printInventory()

            for g in self.groupL:
                memberList = []
                if self.groupNameDict[g] in self.groupMembers:
                    for mem in self.groupMembers[self.groupNameDict[g]]:
                        memberList.append({"value": mem[1]})
                data = {
                    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                    "displayName": "db-temp-" + g,
                    "members": memberList,
                }
                res = requests.post(
                    f"{self.workspace_url}/api/2.0/preview/scim/v2/Groups",
                    headers=self.headers,
                    data=json.dumps(data),
                )
                if res.status_code == 409:
                    print(
                        f'group with name "db-temp-"{g} already present, please delete and try again.'
                    )
                    continue
                self.groupWSGIdDict[res.json()["id"]] = "db-temp-" + g
                self.groupWSGNameDict["db-temp-" + g] = res.json()["id"]
            self.applyGroupPermission("Workspace")
            self.persistInventory("Workspace")
        except Exception as e:
            print(f" Error creating backup groups , {e}")

    def validateAccountGroup(self):
        try:
            res = requests.get(
                f"{self.workspace_url}/api/2.0/account/scim/v2/Groups",
                headers=self.headers,
            )
            for grp in res.json()["Resources"]:
                self.accountGroups_lower[grp["displayName"].casefold()] = grp["id"]
            for g in self.WorkspaceGroupNames:
                if g.casefold() not in self.accountGroups_lower:
                    print(
                        f"group {g} is not present in account level, please add correct group and try again"
                    )
                    return 1
            return 0
        except Exception as e:
            print(f" Error validating account level group, {e}")

    def createAccountGroup(self):
        try:
            if self.validateAccountGroup() == 1:
                return
            if self.validateTempWSGroup() == 0:
                return
            self.performInventory("Account")
            self.printInventory()
            data = {"permissions": ["USER"]}
            for g in self.WorkspaceGroupNames:
                res = requests.put(
                    f"{self.workspace_url}/api/2.0/preview/permissionassignments/principals/{self.accountGroups_lower[g.casefold()]}",
                    headers=self.headers,
                    data=json.dumps(data),
                )
            self.applyGroupPermission("Account")
            self.persistInventory("Account")

        except Exception as e:
            print(f" Error creating account level group, {e}")
