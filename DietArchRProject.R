library(ArchR)

##This function will iterate over the arrows within an ArchRProject, only keeping the matrices selected. 
## The minimum matrix to keep is is the TileMatrix. 
## Use this function if you want a minimal ArchR Project, or to recover a corrupted ArchR Project. 
##### Sometimes, an ArchR project will become corrupted. This can occur when a multithreading application gets interrupted, 
##### leaving some arrows with matrices, that others do not have. Removing those problematic matrices from the ArchR Project will fix any subsetting issues.
DietArchRProject <- function(ArchRProj = NULL, keep = "TileMatrix", verbose = FALSE){	
	if(! any(keep %in% "TileMatrix")){
		stop("You must keep at least the TileMatrix within each arrow file.")
	}else{
		allMatrices <- getAvailableMatrices(ArchRProj)
		for(i in 1:length(allMatrices)){
			if(!allMatrices[i] %in% keep){
				dropGroupsFromProject(ArchRProj, dropGroups = allMatrices[i], verbose = verbose)
			}
		}
	}
	return(ArchRProj)
}


#################################################################################

##### This function will remove all of a given matrix from an ArchRProject
dropGroupsFromProject <- function(
	ArchRProj = NULL,
	dropGroups = NULL,
	level = 0,
	verbose = FALSE,
	logFile = NULL){

	arrowFiles <- getArrowFiles(ArchRProj)
	if(getArchRThreads() == 1){

		for(i in 1:length(arrowFiles)){
			dropGroupsFromArrow(arrowFiles[i], dropGroups=dropGroups, level = level, verbose = verbose, logFile = logFile)
		}
	}else{

		mclapply(arrowFiles, function(x){
			dropGroupsFromArrow(x, dropGroups=dropGroups, level = level, verbose = verbose, logFile = logFile)
		}, mc.cores = getArchRThreads())
	}
}


#######################################################################################################################

########################### Support functions

########################################################################################################################
## This function is adapted from ArchR.
#https://github.com/GreenleafLab/ArchR/blob/968e4421ce7187a8ac7ea1cf6077412126876d5f/R/ArrowUtils.R#L231 

dropGroupsFromArrow <- function(
  ArrowFile = NULL, 
  dropGroups = NULL,
  level = 0,
  verbose = FALSE,
  logFile = NULL
  ){

  tstart <- Sys.time()

  #Summarize Arrow Content
# ArrowInfo <- .summarizeArrowContent(ArrowFile)
  ArrowInfo <- summarizeArrowContent(ArrowFile)

  #.logMessage(".dropGroupsFromArrow : Initializing Temp ArrowFile", logFile = logFile)

  #We need to transfer first
#  outArrow <- .tempfile(fileext = ".arrow")
  outArrow <- tempfile(fileext = ".arrow")
  o <- h5closeAll()
  o <- h5createFile(outArrow)
  o <- h5write(obj = "Arrow", file = outArrow, name = "Class")
  o <- h5write(obj = paste0(packageVersion("ArchR")), file = outArrow, name = "ArchRVersion")

  #1. Metadata First
  #.logMessage(".dropGroupsFromArrow : Adding Metadata to Temp ArrowFile", logFile = logFile)
  groupName <- "Metadata"
  o <- h5createGroup(outArrow, groupName)

  mData <- ArrowInfo[[groupName]]
  
  for(i in seq_len(nrow(mData))){
    h5name <- paste0(groupName, "/", mData$name[i])
    h5write(h5read(ArrowFile, h5name), file = outArrow, name = h5name)
  }

  #2. Other Groups
  #.logMessage(".dropGroupsFromArrow : Adding SubGroups to Temp ArrowFile", logFile = logFile)

  groupsToTransfer <- names(ArrowInfo)
  groupsToTransfer <- groupsToTransfer[groupsToTransfer %ni% "Metadata"]
  if(!is.null(dropGroups)){
    groupsToTransfer <- groupsToTransfer[tolower(groupsToTransfer) %ni% tolower(dropGroups)]
  }

  for(k in seq_along(groupsToTransfer)){

    #.logDiffTime(paste0("Transferring ", groupsToTransfer[k]), tstart, verbose = verbose, logFile = logFile)

    #Create Group
    groupName <- groupsToTransfer[k]
    o <- h5createGroup(outArrow, groupName)
    
    #Sub Data
    mData <- ArrowInfo[[groupName]]
    
    #Get Order Of Sub Groups (Mostly Related to Seqnames)
    seqOrder <- sort(names(mData))
    if(any(grepl("chr", seqOrder))){
      seqOrder <- c(seqOrder[!grepl("chr", seqOrder)], seqOrder[grepl("chr", seqOrder)])
    }
    
    for(j in seq_along(seqOrder)){

      if(verbose) message(j, " ", appendLF = FALSE)

      #Create Group
      groupJ <- paste0(groupName, "/", seqOrder[j])
      o <- h5createGroup(outArrow, groupJ)

      #Sub mData
      mDataj <- mData[[seqOrder[j]]]

      #Transfer Components
      for(i in seq_len(nrow(mDataj))){
        h5name <- paste0(groupJ, "/", mDataj$name[i])
#        .suppressAll(h5write(.h5read(ArrowFile, h5name), file = outArrow, name = h5name, level = level))
	suppressAll(h5write(h5read(ArrowFile, h5name), file = outArrow, name = h5name, level = level))
      }

    }

    gc()
    
    if(verbose) message("")

  }

  #.logMessage(".dropGroupsFromArrow : Move Temp ArrowFile to ArrowFile", logFile = logFile)

  rmf <- file.remove(ArrowFile)
  out <- fileRename(from = outArrow, to = ArrowFile)
  
  #.logDiffTime("Completed Dropping of Group(s)", tstart, logFile = logFile, verbose = verbose)

  ArrowFile

}




##########################################################################

## Below are internal function called by dropGroupsFromArrow

##########################################################################

summarizeArrowContent <- function(ArrowFile = NULL){
  
  o <- h5closeAll()
  
  #Get Contents of ArrowFile
  h5DF <- h5ls(ArrowFile)

  #Re-Organize Content Info
  h5DF <- h5DF[-which(h5DF$group == "/"),]
  groups <- stringr::str_split(h5DF$group, pattern = "/", simplify=TRUE)[,2]
  groupList <- split(h5DF, groups)

  #Split Nested Lists
  groupList2 <- lapply(seq_along(groupList), function(x){
    groupDFx <- groupList[[x]]
    groupx <- gsub(paste0("/", names(groupList)[x]),"",groupDFx$group)
    if(all(groupx=="")){
      groupDFx
    }else{
      subDF <- groupDFx[-which(groupx == ""),]
      split(subDF, stringr::str_split(subDF$group, pattern = "/", simplify=TRUE)[,3])
    }
  })
  names(groupList2) <- names(groupList)


  o <- h5closeAll()

  return(groupList2)

}



fileRename <- function(from = NULL, to = NULL){

  if(!file.exists(from)){
    stop("Input file does not exist!")
  }
  
  tryCatch({
 
    o <- suppressAll(file.rename(from, to))

    if(!o){
      stop("retry with mv")
    }
    
  }, error = function(x){

    tryCatch({

      system(paste0("mv ", from, " ", to))

      return(to)

    }, error = function(y){

      stop("File Moving/Renaming Failed!")

    })

  })

}

suppressAll <- function(expr = NULL){
  suppressPackageStartupMessages(suppressMessages(suppressWarnings(expr)))
}

