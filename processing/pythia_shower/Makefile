# Makefile for Pythia8 shower programs
# =====================================
# Build shower_normal, shower_phi, and event_mixer_multisource
#
# Prerequisites:
#   - CMSSW environment loaded (provides Pythia8, HepMC3)
#   - HepMC2 available for event mixer
#
# Usage:
#   make all        # Build all programs
#   make shower     # Build shower programs only
#   make mixer      # Build event mixer only
#   make clean      # Remove built files

# Compiler settings (allow environment override)
CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall

# Fixed paths on CVMFS for el8_amd64_gcc10 (CMSSW_12_4_14)
# These are stable paths that work inside Singularity containers
PYTHIA8_BASE_EL8 = /cvmfs/cms.cern.ch/el8_amd64_gcc10/external/pythia8/306-494ded5c626b685d055d5b022e918c0c
HEPMC3_BASE_EL8 = /cvmfs/cms.cern.ch/el8_amd64_gcc10/external/hepmc3/3.2.5-c3cd50aeecf06b194814f1a75bf7872e
HEPMC2_BASE_EL8 = /cvmfs/cms.cern.ch/el8_amd64_gcc10/external/hepmc/2.06.10-46867a6dcc6e5712b7953fe57085fcbd

# Use environment variables if set, otherwise use fixed paths
PYTHIA8_INCLUDE = $(if $(PYTHIA8_BASE),$(PYTHIA8_BASE)/include,$(PYTHIA8_BASE_EL8)/include)
PYTHIA8_LIBDIR = $(if $(PYTHIA8_BASE),$(PYTHIA8_BASE)/lib,$(PYTHIA8_BASE_EL8)/lib)
HEPMC3_INCLUDE = $(if $(HEPMC3_BASE),$(HEPMC3_BASE)/include,$(HEPMC3_BASE_EL8)/include)
HEPMC3_LIBDIR = $(if $(HEPMC3_BASE),$(HEPMC3_BASE)/lib64,$(HEPMC3_BASE_EL8)/lib64)
HEPMC2_INCLUDE = $(if $(HEPMC_BASE),$(HEPMC_BASE)/include,$(HEPMC2_BASE_EL8)/include)
HEPMC2_LIBDIR = $(if $(HEPMC_BASE),$(HEPMC_BASE)/lib,$(HEPMC2_BASE_EL8)/lib)

# Targets
SHOWER_PROGS = shower_normal shower_phi shower_sps
MIXER_PROG = event_mixer_multisource
ALL_PROGS = $(SHOWER_PROGS) $(MIXER_PROG)

.PHONY: all shower mixer clean check-env

all: check-env $(ALL_PROGS)

shower: check-env $(SHOWER_PROGS)

mixer: check-env $(MIXER_PROG)

check-env:
	@echo "Build environment:"
	@echo "  CXX:            $(CXX)"
	@echo "  PYTHIA8_INCLUDE: $(PYTHIA8_INCLUDE)"
	@echo "  PYTHIA8_LIBDIR:  $(PYTHIA8_LIBDIR)"
	@echo "  HEPMC3_INCLUDE:  $(HEPMC3_INCLUDE)"
	@echo "  HEPMC3_LIBDIR:   $(HEPMC3_LIBDIR)"

# Shower programs (use Pythia8 and HepMC3)
shower_normal: shower_normal.cc
	@echo "Building shower_normal..."
	$(CXX) $(CXXFLAGS) $< -o $@ \
		-I$(PYTHIA8_INCLUDE) -L$(PYTHIA8_LIBDIR) \
		-I$(HEPMC3_INCLUDE) -L$(HEPMC3_LIBDIR) \
		-Wl,-rpath,$(PYTHIA8_LIBDIR) -Wl,-rpath,$(HEPMC3_LIBDIR) \
		-lpythia8 -lHepMC3
	@echo "Built: $@"

shower_phi: shower_phi.cc
	@echo "Building shower_phi..."
	$(CXX) $(CXXFLAGS) $< -o $@ \
		-I$(PYTHIA8_INCLUDE) -L$(PYTHIA8_LIBDIR) \
		-I$(HEPMC3_INCLUDE) -L$(HEPMC3_LIBDIR) \
		-Wl,-rpath,$(PYTHIA8_LIBDIR) -Wl,-rpath,$(HEPMC3_LIBDIR) \
		-lpythia8 -lHepMC3
	@echo "Built: $@"

shower_sps: shower_sps.cc
	@echo "Building shower_sps..."
	$(CXX) $(CXXFLAGS) $< -o $@ \
		-I$(PYTHIA8_INCLUDE) -L$(PYTHIA8_LIBDIR) \
		-I$(HEPMC3_INCLUDE) -L$(HEPMC3_LIBDIR) \
		-Wl,-rpath,$(PYTHIA8_LIBDIR) -Wl,-rpath,$(HEPMC3_LIBDIR) \
		-lpythia8 -lHepMC3
	@echo "Built: $@"

# Event mixer (uses both HepMC3 and HepMC2)
event_mixer_multisource: event_mixer_multisource.cc
	@echo "Building event_mixer_multisource..."
	$(CXX) $(CXXFLAGS) $< -o $@ \
		-I$(HEPMC3_INCLUDE) -I$(HEPMC2_INCLUDE) \
		-L$(HEPMC3_LIBDIR) -L$(HEPMC2_LIBDIR) \
		-Wl,-rpath,$(HEPMC3_LIBDIR) -Wl,-rpath,$(HEPMC2_LIBDIR) \
		-lHepMC3 -lHepMC
	@echo "Built: $@"

clean:
	rm -f $(ALL_PROGS)
	@echo "Cleaned build files"

# Help target
help:
	@echo "Makefile for Pythia8 shower programs"
	@echo ""
	@echo "Targets:"
	@echo "  all      - Build all programs"
	@echo "  shower   - Build shower_normal, shower_phi, and shower_sps"
	@echo "  mixer    - Build event_mixer_multisource"
	@echo "  clean    - Remove built files"
	@echo ""
	@echo "Environment variables (set by CMSSW):"
	@echo "  CMSSW_BASE  = $(CMSSW_BASE)"
	@echo "  HEPMC3_DIR  = $(HEPMC3_DIR)"
	@echo "  HEPMC2_DIR  = $(HEPMC2_DIR)"
