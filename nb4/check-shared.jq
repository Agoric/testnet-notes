select(.type == "import-kernel-start"
    or .type == "import-kernel-finish"
    or .type == "deliver"
    or .type == "deliver-result"
    )
 | del(.time)
