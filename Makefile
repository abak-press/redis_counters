BUNDLE = bundle
BUNDLE_OPTIONS = -j 4
RSPEC = ${BUNDLE} exec rspec

all: test

test: bundler/install
	${RSPEC} 2>&1

bundler/install:
	if ! gem list bundler -i > /dev/null; then \
	  gem install bundler; \
	fi
	${BUNDLE} install ${BUNDLE_OPTIONS}