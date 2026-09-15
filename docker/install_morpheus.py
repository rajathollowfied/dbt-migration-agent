"""Build-time install of the Morpheus transpiler engine.

Deliberately NOT `databricks labs install lakebridge` (the full CLI flow) --
that installs ~1.25GB of unrelated connectors/docs/tests we never use
(reconcile, Teradata/Oracle/BigQuery connectors, etc.) and requires live
Databricks credentials just to run its interactive install wizard.

Morpheus itself is a public Maven artifact
(com.databricks.labs:databricks-morph-plugin, ~68MB) with its own installer
class that takes no WorkspaceClient at all. Confirmed empirically
(2026-09-15, see CHECKPOINT.md "Deferred bundle/UI work"): this needs zero
Databricks credentials, just Java 21+ and internet access to Maven -- which
is exactly why this can run at `docker build` time, before any user
credentials ever enter the image.
"""

import sys

from databricks.labs.lakebridge.transpiler.installers import MorpheusInstaller
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository

installer = MorpheusInstaller(TranspilerRepository.user_home())
if not installer.is_java_version_okay():
    sys.exit("Java 21+ not found or not on PATH -- install it before this step.")
if not installer.install():
    sys.exit("Morpheus transpiler install failed.")
print("Morpheus transpiler engine installed OK.")
