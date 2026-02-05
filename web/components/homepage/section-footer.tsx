import Link from "next/link";

export function FooterSection() {
  return (
    <footer className="py-12 bg-theme-alt">
      <div className="container mx-auto px-6">
        <div className="grid md:grid-cols-4 gap-8">
          {/* About */}
          <div>
            <h3 className="font-bold mb-4 text-lg">ABOUT</h3>
            <p className="text-sm leading-relaxed">
              FluxMQ is developed by Abstract Machines, an IoT infrastructure
              and security company located in Paris.
            </p>
          </div>

          {/* Products */}
          <div>
            <h3 className="font-bold mb-4 text-lg">PRODUCTS</h3>
            <ul className="space-y-2 text-sm">
              <li>
                <a
                  href="https://magistrala.absmach.eu"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Magistrala
                </a>
              </li>
              <li>
                <a
                  href="https://absmach.eu/supermq/"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  SuperMQ
                </a>
              </li>
              <li>
                <a
                  href="https://absmach.eu/propeller/"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Propeller
                </a>
              </li>
            </ul>
          </div>

          {/* Resources */}
          <div>
            <h3 className="font-bold mb-4 text-lg">RESOURCES</h3>
            <ul className="space-y-2 text-sm">
              <li>
                <Link href="/docs" className="hover:text-(--flux-orange)">
                  Documentation
                </Link>
              </li>
              <li>
                <a
                  href="https://github.com/absmach/fluxmq"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  GitHub
                </a>
              </li>
              <li>
                <a
                  href="https://absmach.eu/blog/"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Blog
                </a>
              </li>
            </ul>
          </div>

          {/* Contact */}
          <div>
            <h3 className="font-bold mb-4 text-lg">CONTACT</h3>
            <ul className="space-y-2 text-sm">
              <li>
                <a
                  href="mailto:info@absmach.eu"
                  className="hover:text-(--flux-orange)"
                >
                  info@absmach.eu
                </a>
              </li>
              <li>
                <a
                  href="https://github.com/absmach"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  GitHub
                </a>
              </li>
              <li>
                <a
                  href="https://twitter.com/absmach"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Twitter
                </a>
              </li>
              <li>
                <a
                  href="https://www.linkedin.com/company/abstract-machines"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  LinkedIn
                </a>
              </li>
            </ul>
          </div>
        </div>

        <div className="border-t border-theme mt-8 pt-8 text-center text-sm opacity-70">
          <p>© 2026 Abstract Machines. Licensed under Apache 2.0.</p>
        </div>
      </div>
    </footer>
  );
}
