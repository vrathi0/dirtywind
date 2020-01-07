
#' Utility ClusterPSOCK function to send jobs to AWS EC2 instances
#' Source: https://davisvaughan.github.io/furrr/articles/advanced-furrr-remote-connections.html

#' @export make_cluster_ec2
make_cluster_ec2  <- function(public_ip){
   cl_multi <- makeClusterPSOCK(

  # Public IP number of EC2 instance
  workers = public_ip,
  ssh_private_key_file = Sys.getenv('PEM_PATH')

  # User name (always 'ubuntu')
  user = "ubuntu",

  # Use private SSH key registered with AWS
  rshopts = c(
    "-o", "StrictHostKeyChecking=no",
    "-o", "IdentitiesOnly=yes",
    "-i", ssh_private_key_file
  ),

  # Set up .libPaths() for the 'ubuntu' user and
  # install furrr
  rscript_args = c(
    "-e", shQuote("local({p <- Sys.getenv('R_LIBS_USER'); dir.create(p, recursive = TRUE, showWarnings = FALSE); .libPaths(p)})"),
    "-e", shQuote("install.packages('furrr')")
  ),

  # Switch this to TRUE to see the code that is run on the workers without
  # making the connection
  dryrun = FALSE)

  return(cl_multi)

}

