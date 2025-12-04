# PipeStream CI/CD Build Pipeline Guide

This document describes the gold standard CI/CD pipeline for PipeStream projects. Follow these guidelines to enable:
- Automated semantic versioning
- GPG-signed Maven Central publishing
- GitHub release creation with assets
- Docker image publishing to GHCR and Docker Hub

## Prerequisites

### Organization-Level Secrets (Already Configured)

These secrets are configured at the `ai-pipestream` organization level and available to all public repositories:

| Secret | Description |
|--------|-------------|
| `GPG_PRIVATE_KEY` | ASCII-armored GPG private key for signing artifacts |
| `GPG_PASSPHRASE` | Passphrase for the GPG key |
| `MAVEN_CENTRAL_USERNAME` | Maven Central (Sonatype) username |
| `MAVEN_CENTRAL_PASSWORD` | Maven Central (Sonatype) password/token |
| `DOCKER_USER` | Docker Hub username (`pipestreamai`) |
| `DOCKER_TOKEN` | Docker Hub access token |
| `DOCKER_NAMESPACE` | Docker Hub namespace (`pipestreamai`) |
| `DISPATCH_TOKEN` | GitHub PAT for triggering repository dispatch events |

## Project Setup

### 0. Initial Setup - Seed Version Tag

Before your first release, create an initial version tag to set the starting point:

```bash
# For a brand new project starting at 0.0.1-SNAPSHOT
git tag -a v0.0.0 -m "Initial base version"
git push origin v0.0.0

# Verify the version
./gradlew currentVersion -q --no-daemon
# Should show: 0.0.0 (or 0.0.1-SNAPSHOT after next commit)
```

The first release (patch) will create `0.0.1`, and subsequent development will be `0.0.2-SNAPSHOT`.

**Note:** If you have existing version tags, delete them first and recreate the base tag to reset versioning.

### 1. Gradle Build Configuration (`build.gradle`)

Things to take note:
- `scmVersion` plugin is used for semantic versioning
- `signing` plugin is used for GPG signing
- `nmcp` plugin is used for Maven Central publishing
- `afterEvaluate` block ensures signing tasks run before NMCP publication
- version set by `scmVersion` plugin
- need to properly set YOUR_PROJECT_NAME and YOUR_PROJECT_DESCRIPTION and YOUR_REPO
- Don't forget to add MIT license
- krickert is the default developer
- ensure the repo names are correct
- repository setup with gihub packages and docker hub
- m2 repo publishing with signed jars for production releases

Add the following plugins and configuration:

```groovy
import org.gradle.plugins.signing.Sign

plugins {
    id 'pl.allegro.tech.build.axion-release' version '1.21.0'
    id 'java'
    id 'maven-publish'
    id 'signing'
    id 'com.gradleup.nmcp' version '1.2.1'
}

// Semantic versioning with axion-release
scmVersion {
    tag {
        prefix = 'v'
    }
    checks {
        uncommittedChanges = false
        aheadOfRemote = false
        snapshotDependencies = false
    }
}

group 'ai.pipestream'
version "${scmVersion.version}"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
    withJavadocJar()
    withSourcesJar()
}

// Publishing configuration
publishing {
    publications {
        create('maven', MavenPublication) {
            from components.java
            pom {
                name.set('YOUR_PROJECT_NAME')  // <-- CUSTOMIZE
                description.set('YOUR_PROJECT_DESCRIPTION')  // <-- CUSTOMIZE
                url.set('https://github.com/ai-pipestream/YOUR_REPO')  // <-- CUSTOMIZE

                licenses {
                    license {
                        name.set('MIT License')
                        url.set('https://opensource.org/license/mit')
                    }
                }

                developers {
                    developer {
                        id.set('krickert')
                        name.set('Pipestream Engine Team')
                    }
                }

                scm {
                    connection.set('scm:git:git://github.com/ai-pipestream/YOUR_REPO.git')  // <-- CUSTOMIZE
                    developerConnection.set('scm:git:ssh://github.com/ai-pipestream/YOUR_REPO.git')  // <-- CUSTOMIZE
                    url.set('https://github.com/ai-pipestream/YOUR_REPO')  // <-- CUSTOMIZE
                }
            }
        }
    }

    repositories {
        mavenLocal()

        // GitHub Packages (optional)
        def ghRepo = System.getenv('GITHUB_REPOSITORY') ?: 'ai-pipestream/YOUR_REPO'  // <-- CUSTOMIZE
        def ghActor = System.getenv('GITHUB_ACTOR') ?: (project.findProperty('gpr.user') ?: System.getenv('GPR_USER'))
        def ghToken = System.getenv('GITHUB_TOKEN') ?: (project.findProperty('gpr.key') ?: System.getenv('GPR_TOKEN'))
        if (ghActor && ghToken) {
            maven {
                name = 'GitHubPackages'
                url = uri("https://maven.pkg.github.com/${ghRepo}")
                credentials {
                    username = ghActor
                    password = ghToken
                }
            }
        }
    }
}

// GPG Signing (uses org-level secrets)
signing {
    def signingKey = System.getenv("GPG_PRIVATE_KEY")
    def signingPassword = System.getenv("GPG_PASSPHRASE")

    if (signingKey && signingPassword) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign publishing.publications.maven
    }
}

// Ensure signing tasks run before NMCP publication
afterEvaluate {
    tasks.matching { it.name.startsWith("nmcpZipAllPublications") || it.name.startsWith("nmcpPublishAllPublicationsToCentralPortal") }
        .configureEach { nmcpTask ->
            nmcpTask.dependsOn(tasks.withType(Sign))
        }
}

// Maven Central publishing via NMCP
nmcp {
    publishAllPublicationsToCentralPortal {
        username = providers.environmentVariable("MAVEN_CENTRAL_USERNAME")
        password = providers.environmentVariable("MAVEN_CENTRAL_PASSWORD")
        publishingType = "AUTOMATIC"
    }
}
```

### 2. GitHub Workflows

Copy these workflow files to `.github/workflows/`:

#### `build-and-publish.yml`
- Triggers on: push to main, pull requests
- Actions: Build, test, publish snapshots to GitHub Packages
- See: [build-and-publish.yml](.github/workflows/build-and-publish.yml)

https://raw.githubusercontent.com/ai-pipestream/platform-registration-service/refs/heads/main/.github/workflows/build-and-publish.yml

(This is from the platform-registration-service, adjust as needed)

#### `release-and-publish.yml`
- Triggers on: manual workflow dispatch
- Actions: Create release tag, publish to Maven Central, create GitHub release, build and push Docker image
- See: [release-and-publish.yml](.github/workflows/release-and-publish.yml)

https://raw.githubusercontent.com/ai-pipestream/platform-registration-service/refs/heads/main/.github/workflows/release-and-publish.yml

(This is from the platform-registration-service, adjust as needed)


#### `dockerhub-publish.yml`
- Triggers on: repository dispatch from release workflow
- Actions: Pull from GHCR, push to Docker Hub
- See: [dockerhub-publish.yml](.github/workflows/dockerhub-publish.yml)

https://raw.githubusercontent.com/ai-pipestream/platform-registration-service/refs/heads/main/.github/workflows/dockerhub-publish.yml

(This is from the platform-registration-service, adjust as needed)

### 3. Customization Checklist

For each new project, update these values:

**In `build.gradle`:**
- [ ] `pom.name` - Human-readable project name
- [ ] `pom.description` - Project description
- [ ] `pom.url` - GitHub repository URL
- [ ] `pom.scm.*` - SCM connection URLs
- [ ] `ghRepo` default value - Repository path

**In `release-and-publish.yml`:**
- [ ] Docker image name in `quarkus.container-image.name`
- [ ] Project name in release notes body
- [ ] Maven coordinates in release notes
- [ ] Docker image URLs in release notes
- [ ] Asset zip file name pattern

**In `build-and-publish.yml`:**
- [ ] Docker image name in `quarkus.container-image.name`

**In `dockerhub-publish.yml`:**
- [ ] Verify Docker Hub repository name matches project

**In `.gitignore`:**
- [ ] Add `.dev-helpers/` for auto-downloaded helper scripts
- [ ] Add `.claude` and `CLAUDE.md` for Claude Code artifacts
- [ ] Ensure no secrets or local config files are tracked

Example `.gitignore` additions:
```gitignore
# Claude Code
.claude
CLAUDE.md

# Auto-downloaded dev helpers
.dev-helpers/
```

**In startup scripts (`scripts/start-*.sh`):**
- [ ] Update `SERVICE_NAME` for your service
- [ ] Update `SERVICE_PORT` to match your service port
- [ ] Update `DESCRIPTION` with service description
- [ ] Ensure script uses bootstrap pattern (auto-downloads from GitHub)

### 4. Key Workflow Features

**Improved Version Parsing:**
- Strips "Project version: " prefix from Gradle output using `${CURRENT#Project version: }`
- Removes -SNAPSHOT suffix using `${CURRENT%-SNAPSHOT}`
- Uses bash parameter expansion instead of sed for cleaner parsing
- Example:
  ```bash
  CURRENT=$(./gradlew currentVersion -q --no-daemon | tail -1)
  CURRENT=${CURRENT#Project version: }  # Strip prefix
  CURRENT=${CURRENT%-SNAPSHOT}           # Strip suffix
  ```

**Better Release Notes:**
- Uses `softprops/action-gh-release@v1` for rich release notes
- Includes Maven coordinates in XML format
- Shows both GHCR and Docker Hub image URLs
- Automatically attaches zip artifacts

**Optimized Publishing:**
- `-PskipCentral=true` prevents duplicate Maven Central attempts
- Separate steps for Central and GitHub Packages
- `contents: write` permission on docker jobs for proper access

**Hardcoded Organization Names:**
- Uses `ai-pipestream` and `pipestreamai` directly
- More explicit and avoids potential variable resolution issues

**Optional Documentation Files:**
- Uses `cp DOCKER.md release-assets/ 2>/dev/null || true` for optional docs
- Prevents build failures if documentation files don't exist
- Only README.md is required; DOCKER.md/DOCKER_RUN.md are optional

## Release Process

### Creating a Release

1. **Ensure main branch is up to date and all tests pass**

2. **Trigger the release workflow:**
   - Go to Actions > "Release and Publish"
   - Click "Run workflow"
   - Select release type: `patch`, `minor`, `major`, or `manual`
   - For manual, specify exact version (e.g., `0.0.1`)

3. **Workflow automatically:**
   - Builds and tests the project
   - Creates git tag (e.g., `v0.0.1`)
   - Signs and publishes to Maven Central
   - Publishes to GitHub Packages
   - Creates GitHub release with assets
   - Builds and pushes Docker image to GHCR
   - Triggers Docker Hub mirroring

### Manual Docker Hub Push

If you need to manually trigger Docker Hub mirroring:

```bash
echo '{"event_type":"dockerhub-publish","client_payload":{"base_image":"ghcr.io/ai-pipestream/YOUR_IMAGE","dockerhub_repo":"pipestreamai/YOUR_IMAGE","tags":"VERSION latest"}}' | \
  gh api repos/ai-pipestream/YOUR_REPO/dispatches --input -
```

### Checking Maven Central

After release, verify on:
- https://central.sonatype.com/namespace/ai.pipestream
- https://repo1.maven.org/maven2/ai/pipestream/YOUR_ARTIFACT/

Note: Maven Central sync can take 15-30 minutes.

## Troubleshooting

### Missing GPG Signatures

**Error:** `Missing signature for file: *.jar`

**Cause:** GPG_PRIVATE_KEY or GPG_PASSPHRASE environment variables not set

**Fix:** Ensure org secrets are correctly named and accessible to the repository

### Repository Dispatch Fails

**Error:** `Resource not accessible by integration`

**Cause:** GITHUB_TOKEN cannot trigger repository_dispatch

**Fix:** Use DISPATCH_TOKEN (PAT with repo scope) instead

### Nested configureEach Error

**Error:** `DefaultTaskCollection#configureEach(Action) on task set cannot be executed in the current context`

**Cause:** Gradle 9+ restricts nested configureEach calls

**Fix:** Wrap in `afterEvaluate` block (see build.gradle example above)

### Docker Login Fails

**Error:** `unauthorized: incorrect username or password`

**Fix:** For organization Docker Hub accounts, use the organization name as username (e.g., `pipestreamai`), not personal username

### GHCR Package Write Permission Denied

**Error:** `denied: permission_denied: write_package`

**Cause:** The GHCR package doesn't grant workflow access to the repository

**Fix:** After first release creates the package:
1. Go to: `https://github.com/orgs/ai-pipestream/packages/container/YOUR_PACKAGE/settings`
2. Under "Manage Actions access", click **"Add Repository"**
3. Add your repository with **Write** access
4. Re-run the failed job

This is a one-time setup per package. The release workflow creates the package, but snapshot builds need explicit permission.

### Version Parsing Issues

**Error:** Version shows `0.1.0-SNAPSHOT` instead of expected value

**Cause:** No version tags exist or tag format is incorrect

**Fix:**
```bash
# Check existing tags
git tag -l 'v*'

# Create proper base tag
git tag -a v0.0.0 -m "Initial base version"
git push origin v0.0.0
```

## Best Practices

1. **Always test locally first** before pushing workflow changes
2. **Keep secrets at org level** to avoid per-repo configuration
3. **Use semantic versioning** - let axion-release manage versions
4. **Don't skip signing** - Maven Central requires signed artifacts
5. **Monitor workflow runs** - Check for deprecation warnings
6. **Clean up failed tags** - If a release fails mid-way, delete the tag before retrying

## Files Structure

```
your-project/
├── .github/
│   └── workflows/
│       ├── build-and-publish.yml      # CI on push/PR
│       ├── release-and-publish.yml    # Release workflow
│       └── dockerhub-publish.yml      # Docker Hub mirroring
├── scripts/
│   └── start-your-service.sh          # Dev startup with bootstrap
├── build.gradle                        # Build config with signing/publishing
├── settings.gradle                     # Project settings
├── .gitignore                          # Includes .dev-helpers/, .claude, etc.
└── BUILD_PIPELINE.md                   # This documentation (optional)
```

## Startup Script Pattern

Each service includes a self-bootstrapping startup script that:

1. **Auto-downloads helper scripts** from `ai-pipestream/dev-assets` GitHub repo
2. **Caches helpers** in `.dev-helpers/` (gitignored)
3. **Creates fallback helpers** if network unavailable
4. **Works OOTB** after git clone - no manual setup required

Key features:
- Uses `DEV_ASSETS_LOCATION_OVERRIDE` env var for custom paths
- Caches downloaded scripts for offline use
- Provides minimal fallback functions if download fails
- Pattern similar to Gradle Wrapper for consistency

Example bootstrap pattern (in `scripts/start-*.sh`):
```bash
DEV_ASSETS_REPO="https://raw.githubusercontent.com/ai-pipestream/dev-assets/main"
HELPERS_DIR="$PROJECT_ROOT/.dev-helpers"

bootstrap_helpers() {
  if [ -f "$HELPERS_DIR/scripts/shared-utils.sh" ]; then
    DEV_ASSETS_LOCATION="$HELPERS_DIR"
    return 0
  fi

  # Download from GitHub
  curl -fsSL "$DEV_ASSETS_REPO/scripts/shared-utils.sh" -o "$HELPERS_DIR/scripts/shared-utils.sh"
}
```

## Support

For issues with the build pipeline:
1. Check GitHub Actions logs for specific errors
2. Verify all org secrets are configured
3. Ensure Gradle wrapper is executable (`chmod +x gradlew`)
4. Review this documentation for common issues

---

*Last updated: November 2025*
*Gold standard based on: platform-registration-service*
