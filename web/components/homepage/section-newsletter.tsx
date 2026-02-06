export function NewsletterSection() {
  return (
    <section className="py-20 border-b-2 border-theme bg-theme-alt">
      <div className="container mx-auto px-6">
        <div className="max-w-2xl mx-auto">
          <h2 className="text-3xl md:text-4xl font-bold mb-6 text-center">
            SUBSCRIBE TO NEWSLETTER
          </h2>
          <p className="text-center text-theme-muted mb-8">
            Stay updated with the latest FluxMQ news, updates and announcements.
          </p>

          <form
            action="https://absmach.us11.list-manage.com/subscribe/post?u=70b43c7181d005024187bfb31&amp;id=0a319b6b63&amp;f_id=00d816e1f0"
            method="post"
            id="mc-embedded-subscribe-form"
            name="mc-embedded-subscribe-form"
            target="_blank"
            className="max-w-md mx-auto"
          >
            <div className="flex gap-0">
              <input
                type="email"
                name="EMAIL"
                placeholder="Enter your email"
                required
                className="flex-1 brutalist-border border-r-0 px-4 py-3 focus:outline-none focus:border-(--flux-orange)"
              />
              <button
                type="submit"
                className="brutalist-border bg-(--flux-orange) hover:bg-(--flux-blue) text-white px-6 py-3 font-bold transition-colors"
              >
                SUBSCRIBE
              </button>
            </div>
            <input type="hidden" name="tags" value="8115284" />
            <div
              style={{ position: "absolute", left: "-5000px" }}
              aria-hidden="true"
            >
              <input
                type="text"
                name="b_70b43c7181d005024187bfb31_0a319b6b63"
                tabIndex={-1}
                value=""
              />
            </div>
            <p className="text-xs text-theme-muted mt-4 text-center">
              By subscribing, you agree to our{" "}
              <a
                href="https://absmach.eu/privacy/"
                className="underline"
                target="_blank"
                rel="noopener noreferrer"
              >
                Privacy Policy
              </a>{" "}
              and{" "}
              <a
                href="https://absmach.eu/terms/"
                className="underline"
                target="_blank"
                rel="noopener noreferrer"
              >
                Terms of Service
              </a>
              . You can unsubscribe at any time.
            </p>
          </form>
        </div>
      </div>
    </section>
  );
}
