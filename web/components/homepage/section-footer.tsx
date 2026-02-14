import Link from "next/link";
import Image from "next/image";
import Logo from "@/public/abstract-machines.svg";

export function FooterSection() {
  return (
    <footer className="py-12 bg-theme-alt">
      <div className="container mx-auto px-6">
        <div className="grid md:grid-cols-4 gap-12 items-start">
          {/* About */}
          <div>
            <Link
              href="https://absmach.eu?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
              target="_blank"
            >
              <Image
                src={Logo}
                alt="Magistrala"
                height={120}
                width={120}
                className="invert dark:invert-0 mb-4"
              />
            </Link>
            <p className="text-muted-foreground leading-relaxed mb-5">
              Abstract Machines, creators of FluxMQ, builds secure,
              open-source infrastructure for distributed cloud and edge systems.
            </p>
          </div>

          {/* Products */}
          <div>
            <h3 className="font-bold mb-4 text-lg">PRODUCTS</h3>
            <ul className="space-y-4 text-sm">
              <li>
                <a
                  href="https://magistrala.absmach.eu?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                >
                  Magistrala
                </a>
              </li>
              <li>
                <a
                  href="https://absmach.eu/supermq/?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                >
                  SuperMQ
                </a>
              </li>
              <li>
                <a
                  href="https://propeller.absmach.eu?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                >
                  Propeller
                </a>
              </li>
              <li>
                <a
                  href="https://hardware.absmach.eu?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                >
                  Hardware
                </a>
              </li>
            </ul>
          </div>

          {/* Resources */}
          <div>
            <h3 className="font-bold mb-4 text-lg">RESOURCES</h3>
            <ul className="space-y-4 text-sm">
              <li>
                <Link href="/docs" className="hover:text-(--flux-orange)">
                  Documentation
                </Link>
              </li>
              <li>
                <a
                  href="https://github.com/absmach/fluxmq?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  GitHub
                </a>
              </li>
              <li>
                <a
                  href="https://absmach.eu/blog/?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
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
            <ul className="space-y-4 text-sm">
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
                  href="https://github.com/absmach?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  GitHub
                </a>
              </li>
              <li>
                <a
                  href="https://twitter.com/absmach?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
                  className="hover:text-(--flux-orange)"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Twitter
                </a>
              </li>
              <li>
                <a
                  href="https://www.linkedin.com/company/abstract-machines?utm_source=flux.absmach.eu&utm_medium=website&utm_campaign=footer"
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
