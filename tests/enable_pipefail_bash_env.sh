# Source through BASH_ENV to test production scripts with pipefail enabled
# without also exporting the caller's errexit or nounset shell options.
set -o pipefail
