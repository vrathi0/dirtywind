
#' Utility ClusterPSOCK function to send jobs to AWS EC2 instances
#' Source: https://davisvaughan.github.io/furrr/articles/advanced-furrr-remote-connections.html

#' @export make_cluster_ec2
make_cluster_ec2  <- function(public_ip){
  ssh_private_key_file  <-  Sys.getenv('PEM_PATH')
  github_pac  <-  Sys.getenv('PAC')

  cl_multi <- future::makeClusterPSOCK(
  workers = public_ip,
  user = "ubuntu",
  rshopts = c(
    "-o", "StrictHostKeyChecking=no",
    "-o", "IdentitiesOnly=yes",
    "-i", ssh_private_key_file
  ),
  rscript_args = c(
    "-e", shQuote("local({p <- Sys.getenv('R_LIBS_USER'); dir.create(p, recursive = TRUE, showWarnings = FALSE); .libPaths(p)})"),
    "-e", shQuote("install.packages('furrr')"),
    "-e", shQuote(glue::glue("devtools::install_github('cicala-projects/dirtywind', auth_token = {github_pac})"))
  ),
  dryrun = FALSE)

  return(cl_multi)

}

